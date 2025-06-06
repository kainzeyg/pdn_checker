package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
)

type TableInfo struct {
	SchemaName string
	TableName  string
	TableType  string
}

type ColumnInfo struct {
	ColumnName string
	DataType   string
}

type ValuePattern struct {
	Value   string
	Pattern string
}

type PDNResult struct {
	DatabaseName string
	SchemaName   string
	TableName    string
	TableType    string
	ColumnName   string
	FoundIn      string
	SampleValue  string
	Pattern      string
	PDNType      string
}

func main() {
	server, port, database, username, password := getConnectionParams()
	db := connectToDB(server, port, database, username, password)
	defer db.Close()

	tables := getTablesAndViews(db)
	fmt.Printf("\nНайдено %d таблиц/представлений для анализа\n", len(tables))

	resultsChan := make(chan PDNResult, 1000)
	doneChan := make(chan bool)

	// Генерируем имя файла с сервером и базой
	reportFileName := fmt.Sprintf("report_%s_%s.csv", strings.ReplaceAll(server, "\\", "_"), database)

	go func() {
		err := saveResultsToCSVBatches(server, reportFileName, resultsChan)
		if err != nil {
			log.Fatal("Ошибка сохранения в CSV:", err)
		}
		doneChan <- true
	}()

	analyzeTablesWithBatches(db, database, tables, resultsChan)

	close(resultsChan)
	<-doneChan

	fmt.Printf("\nОтчет успешно сохранен в %s\n", reportFileName)
}

func getConnectionParams() (string, string, string, string, string) {
	var server, port, database, username, password string

	fmt.Print("Введите сервер БД: ")
	fmt.Scanln(&server)
	fmt.Print("Введите порт БД: ")
	fmt.Scanln(&port)
	fmt.Print("Введите имя БД: ")
	fmt.Scanln(&database)
	fmt.Print("Введите логин: ")
	fmt.Scanln(&username)
	fmt.Print("Введите пароль: ")
	fmt.Scanln(&password)

	return server, port, database, username, password
}

func connectToDB(server, port, database, username, password string) *sql.DB {
	connString := fmt.Sprintf("server=%s;port=%s;database=%s;user id=%s;password=%s",
		server, port, database, username, password)

	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatal("Ошибка подключения:", err)
	}

	db.SetConnMaxLifetime(10 * time.Minute)
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		log.Fatal("Ошибка проверки подключения:", err)
	}

	fmt.Println("✓ Успешное подключение к БД")
	return db
}

func getTablesAndViews(db *sql.DB) []TableInfo {
	fmt.Println("\nПолучение списка таблиц и представлений...")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	query := `
		SELECT s.name AS schema_name, t.name AS table_name, t.type_desc AS table_type
		FROM sys.tables t
		INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
		UNION ALL
		SELECT s.name AS schema_name, v.name AS view_name, 'VIEW' AS table_type
		FROM sys.views v
		INNER JOIN sys.schemas s ON v.schema_id = s.schema_id
	`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		log.Fatal("Ошибка получения таблиц:", err)
	}
	defer rows.Close()

	var tables []TableInfo
	for rows.Next() {
		var ti TableInfo
		if err := rows.Scan(&ti.SchemaName, &ti.TableName, &ti.TableType); err != nil {
			log.Println("Ошибка чтения данных таблицы:", err)
			continue
		}
		tables = append(tables, ti)
	}

	return tables
}

func analyzeTablesWithBatches(db *sql.DB, database string, tables []TableInfo, resultsChan chan<- PDNResult) {
	totalTables := len(tables)

	for i, table := range tables {
		fmt.Printf("\n[%d/%d] Анализ %s.%s (%s)...\n",
			i+1, totalTables, table.SchemaName, table.TableName, table.TableType)

		tableCtx, tableCancel := context.WithTimeout(context.Background(), 3*time.Minute)

		columns, err := getColumns(tableCtx, db, table.SchemaName, table.TableName)
		if err != nil {
			log.Printf("⚠ Ошибка получения колонок: %v - пропускаем\n", err)
			tableCancel()
			resultsChan <- createTableTimeoutResult(database, table)
			continue
		}

		fmt.Printf("  Найдено %d колонок\n", len(columns))
		for _, col := range columns {
			fmt.Printf("  - %s (%s)\n", col.ColumnName, col.DataType)
		}

		var allTableResults []PDNResult
		columnResultsChan := make(chan []PDNResult, len(columns))
		errorChan := make(chan error, len(columns))

		for _, column := range columns {
			go func(col ColumnInfo) {
				ctx, cancel := context.WithTimeout(tableCtx, 30*time.Second)
				defer cancel()

				res, err := analyzeColumn(ctx, db, database, table, col)
				if err != nil {
					errorChan <- err
					columnResultsChan <- nil
					return
				}
				columnResultsChan <- res
				errorChan <- nil
			}(column)
		}

		processedColumns := make(map[string]bool)
		for range columns {
			select {
			case res := <-columnResultsChan:
				if res != nil {
					allTableResults = append(allTableResults, res...)
					for _, r := range res {
						resultsChan <- r
						processedColumns[r.ColumnName] = true
					}
				}
			case <-tableCtx.Done():
				fmt.Printf("  ⚠ Превышено время обработки таблицы %s.%s\n",
					table.SchemaName, table.TableName)
			}
		}

		hasOtherPersonalData := false
		for _, res := range allTableResults {
			if res.PDNType != "Адрес" && res.PDNType != "Нет" && res.PDNType != "Не обработано" {
				hasOtherPersonalData = true
				break
			}
		}

		if !hasOtherPersonalData {
			for i, res := range allTableResults {
				if res.PDNType == "Адрес" {
					allTableResults[i].PDNType = "Нет"
				}
			}
		}

		fmt.Println("  Итоги по таблице:")
		hasPDN := false
		for _, res := range allTableResults {
			if res.PDNType != "Нет" && res.PDNType != "Не обработано" {
				fmt.Printf("    * %s: %s (%s)\n", res.ColumnName, res.PDNType, res.FoundIn)
				hasPDN = true
			}
		}
		if !hasPDN {
			fmt.Println("    * Персональные данные не обнаружены")
		}

		for _, column := range columns {
			if !processedColumns[column.ColumnName] {
				resultsChan <- PDNResult{
					DatabaseName: database,
					SchemaName:   table.SchemaName,
					TableName:    table.TableName,
					TableType:    table.TableType,
					ColumnName:   column.ColumnName,
					FoundIn:      "timeout",
					SampleValue:  "N/A",
					Pattern:      "Превышено время обработки",
					PDNType:      "Не обработано",
				}
			}
		}

		tableCancel()
	}
}

func createTableTimeoutResult(database string, table TableInfo) PDNResult {
	return PDNResult{
		DatabaseName: database,
		SchemaName:   table.SchemaName,
		TableName:    table.TableName,
		TableType:    table.TableType,
		ColumnName:   "ALL_COLUMNS",
		FoundIn:      "timeout",
		SampleValue:  "N/A",
		Pattern:      "Превышено время обработки таблицы",
		PDNType:      "Не обработано",
	}
}

func getColumns(ctx context.Context, db *sql.DB, schemaName, tableName string) ([]ColumnInfo, error) {
	query := `
		SELECT c.name AS column_name, tp.name AS data_type
		FROM sys.columns c
		JOIN sys.objects o ON c.object_id = o.object_id
		JOIN sys.schemas s ON o.schema_id = s.schema_id
		JOIN sys.types tp ON c.user_type_id = tp.user_type_id
		WHERE s.name = @schema AND o.name = @table
	`

	rows, err := db.QueryContext(ctx, query,
		sql.Named("schema", schemaName),
		sql.Named("table", tableName))
	if err != nil {
		return nil, fmt.Errorf("запрос колонок: %v", err)
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var ci ColumnInfo
		if err := rows.Scan(&ci.ColumnName, &ci.DataType); err != nil {
			return nil, fmt.Errorf("чтение колонки: %v", err)
		}
		columns = append(columns, ci)
	}

	return columns, nil
}

func analyzeColumn(ctx context.Context, db *sql.DB, database string, table TableInfo, column ColumnInfo) ([]PDNResult, error) {
	var results []PDNResult

	values, err := getSampleValues(ctx, db, table.SchemaName, table.TableName, column.ColumnName)
	if err != nil {
		log.Printf("  Ошибка получения значений для %s.%s (%s): %v",
			table.TableName, column.ColumnName, column.DataType, err)

		results = append(results, PDNResult{
			DatabaseName: database,
			SchemaName:   table.SchemaName,
			TableName:    table.TableName,
			TableType:    table.TableType,
			ColumnName:   column.ColumnName,
			FoundIn:      "error",
			SampleValue:  "N/A",
			Pattern:      fmt.Sprintf("Ошибка получения значений: %v", err),
			PDNType:      "Не обработано",
		})

		return results, nil
	}

	sampleValue := "N/A"
	if len(values) > 0 {
		sampleValue = values[0].Value
	}

	pdnTypes := checkForPDNPatterns(column.ColumnName)
	if len(pdnTypes) > 0 {
		for _, pdnType := range pdnTypes {
			res := PDNResult{
				DatabaseName: database,
				SchemaName:   table.SchemaName,
				TableName:    table.TableName,
				TableType:    table.TableType,
				ColumnName:   column.ColumnName,
				FoundIn:      "header",
				SampleValue:  sampleValue,
				PDNType:      pdnType,
			}
			if len(values) > 0 {
				res.Pattern = values[0].Pattern
			}
			results = append(results, res)
		}
	}

	var valuePdnTypes []string
	for _, val := range values {
		if types := checkForPDNPatterns(val.Value); len(types) > 0 {
			valuePdnTypes = appendIfNotExists(valuePdnTypes, types...)
			for _, pdnType := range types {
				results = append(results, PDNResult{
					DatabaseName: database,
					SchemaName:   table.SchemaName,
					TableName:    table.TableName,
					TableType:    table.TableType,
					ColumnName:   column.ColumnName,
					FoundIn:      "value",
					SampleValue:  val.Value,
					Pattern:      val.Pattern,
					PDNType:      pdnType,
				})
			}
		}
	}

	if len(pdnTypes) == 0 && len(valuePdnTypes) == 0 {
		res := PDNResult{
			DatabaseName: database,
			SchemaName:   table.SchemaName,
			TableName:    table.TableName,
			TableType:    table.TableType,
			ColumnName:   column.ColumnName,
			FoundIn:      "none",
			SampleValue:  sampleValue,
			PDNType:      "Нет",
		}
		if len(values) > 0 {
			res.Pattern = values[0].Pattern
		}
		results = append(results, res)
	}

	return results, nil
}

func getSampleValues(ctx context.Context, db *sql.DB, schemaName, tableName, columnName string) ([]ValuePattern, error) {
	// Пытаемся получить значения как строку
	query := fmt.Sprintf(`
		SELECT TOP 5 TRY_CAST([%s] AS NVARCHAR(MAX)) AS sample_value
		FROM [%s].[%s] WITH (NOLOCK)
		WHERE [%s] IS NOT NULL AND TRY_CAST([%s] AS NVARCHAR(MAX)) != ''
	`, columnName, schemaName, tableName, columnName, columnName)

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		// Если ошибка, пробуем альтернативный вариант с CONVERT
		query = fmt.Sprintf(`
			SELECT TOP 5 CONVERT(NVARCHAR(MAX), [%s]) AS sample_value
			FROM [%s].[%s] WITH (NOLOCK)
			WHERE [%s] IS NOT NULL AND CONVERT(NVARCHAR(MAX), [%s]) != ''
		`, columnName, schemaName, tableName, columnName, columnName)

		rows, err = db.QueryContext(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("запрос значений: %v", err)
		}
	}
	defer rows.Close()

	var values []string
	for rows.Next() {
		var val string
		if err := rows.Scan(&val); err != nil {
			return nil, fmt.Errorf("чтение значения: %v", err)
		}
		values = append(values, val)
	}

	// Если нет значений, проверяем, есть ли вообще данные в колонке
	if len(values) == 0 {
		checkQuery := fmt.Sprintf(`
			SELECT TOP 1 1 
			FROM [%s].[%s] WITH (NOLOCK)
			WHERE [%s] IS NOT NULL AND 
				  (TRY_CAST([%s] AS NVARCHAR(MAX)) IS NOT NULL AND 
				   TRY_CAST([%s] AS NVARCHAR(MAX)) != '')
		`, schemaName, tableName, columnName, columnName, columnName)

		var exists int
		err := db.QueryRowContext(ctx, checkQuery).Scan(&exists)
		if err != nil {
			if err == sql.ErrNoRows {
				return nil, nil // Колонка пустая или содержит только NULL/пустые значения
			}

			// Пробуем альтернативный вариант проверки
			checkQuery = fmt.Sprintf(`
				SELECT TOP 1 1 
				FROM [%s].[%s] WITH (NOLOCK)
				WHERE [%s] IS NOT NULL
			`, schemaName, tableName, columnName)

			err = db.QueryRowContext(ctx, checkQuery).Scan(&exists)
			if err != nil {
				if err == sql.ErrNoRows {
					return nil, nil
				}
				return nil, fmt.Errorf("проверка наличия данных: %v", err)
			}
		}
	}

	patternMap := make(map[string]string)
	for _, val := range values {
		pattern := getValuePattern(val)
		if _, exists := patternMap[pattern]; !exists {
			patternMap[pattern] = val
		}
	}

	var result []ValuePattern
	for pattern, val := range patternMap {
		result = append(result, ValuePattern{
			Value:   val,
			Pattern: pattern,
		})
	}

	return result, nil
}

func checkForPDNPatterns(input string) []string {
	input = strings.ToLower(input)
	var foundTypes []string

	valuePatterns := map[string]*regexp.Regexp{
		"Email":           regexp.MustCompile(`[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}`),
		"Телефон":         regexp.MustCompile(`(\+7|8)[\s\-\(]?\d{3}[\)\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}`),
		"Паспорт РФ":      regexp.MustCompile(`\b(\d{2}\s?\d{2}\s?\d{6}|\d{10})\b|(?:паспорт|серия|номер)[^\d]*(\d{4})[^\d]*(\d{6})`),
		"СНИЛС":           regexp.MustCompile(`\b\d{3}[-]?\d{3}[-]?\d{3}[-\s]?\d{2}\b`),
		"ИНН физлица":     regexp.MustCompile(`(^|\D)\d{12}($|\D)`),
		"Кредитная карта": regexp.MustCompile(`\d{4}[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?\d{4}`),
	}

	headerPatterns := map[string][]string{
		"ФИО":                 {"фамил", "fami", "surn", "lastname", "last name", "last_name", "имя", "firstname", "first name", "first_name", "отчест", "middlename", "middle name", "middle_name", "patronym", "фам", "fio", "фио", "fullname", "full name"},
		"Персональные данные": {"контакт", "сотруд", "руковод", "manag", "физи", "физл", "персон", "person", "empl"},
		"Адрес":               {"адрес", "address", "addr", "location", "место"},
		"Email":               {"эп", "mail", "адресэп", "адрес эп"},
		"Телефон":             {"телефон", "phone", "tel", "мобильн", "mobile", "contact"},
		"Паспорт":             {"паспор", "passpor", "серия", "series"},
		"СНИЛС/ИНН":           {"снилс", "snils", "инн", "taxid", "tax id"},
		"Дата рождения":       {"рожд", "birth", "dateofbirth", "birthdate", "датарожд", "дата рожд"},
		"Таб. номер":          {"таб", "табель"},
		"Фото":                {"фото", "foto", "photo"},
	}

	for pdnType, re := range valuePatterns {
		if re.MatchString(input) {
			foundTypes = append(foundTypes, pdnType)
		}
	}

	for pdnType, keywords := range headerPatterns {
		for _, keyword := range keywords {
			if strings.Contains(input, keyword) {
				foundTypes = appendIfNotExists(foundTypes, pdnType)
			}
		}
	}

	if containsAny(input, []string{"ул.", "улица", "дом", "кв.", "квартира"}) {
		foundTypes = appendIfNotExists(foundTypes, "Адрес")
	}

	if containsAny(input, []string{"рожден", "birthday"}) {
		foundTypes = appendIfNotExists(foundTypes, "Дата рождения")
	}

	return foundTypes
}

func maskSensitiveData(value string) string {
	if value == "N/A" {
		return value
	}
	if len(value) > 8 {
		return value[:4] + "****" + value[len(value)-4:]
	}
	return "****"
}

func containsAny(s string, substrings []string) bool {
	for _, sub := range substrings {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

func appendIfNotExists(slice []string, items ...string) []string {
	for _, item := range items {
		if !contains(slice, item) {
			slice = append(slice, item)
		}
	}
	return slice
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func getValuePattern(value string) string {
	var pattern []rune
	for _, r := range value {
		switch {
		case r >= 'а' && r <= 'я' || r >= 'А' && r <= 'Я' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z':
			pattern = append(pattern, 'A')
		case r >= '0' && r <= '9':
			pattern = append(pattern, '9')
		default:
			pattern = append(pattern, '#')
		}
	}
	return string(pattern)
}

func saveResultsToCSVBatches(server, fileName string, resultsChan <-chan PDNResult) error {
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	header := []string{
		"Сервер",
		"БД",
		"Схема",
		"Таблица/Представление",
		"Тип объекта",
		"Колонка",
		"ПДн (Да\\Нет)",
		"Тип ПДн",
		"Пример значения",
		"Пример значения с маскированием",
	}
	if err := writer.Write(header); err != nil {
		return err
	}

	batchSize := 100
	batchCount := 0

	for result := range resultsChan {
		hasPDN := "Да"
		if result.PDNType == "Нет" || result.PDNType == "Не обработано" {
			hasPDN = "Нет"
		}

		record := []string{
			server,
			result.DatabaseName,
			result.SchemaName,
			result.TableName,
			result.TableType,
			result.ColumnName,
			hasPDN,
			result.PDNType,
			result.SampleValue,
			maskSensitiveData(result.SampleValue),
		}

		if err := writer.Write(record); err != nil {
			return err
		}

		batchCount++
		if batchCount%batchSize == 0 {
			writer.Flush()
			if err := writer.Error(); err != nil {
				return err
			}
			log.Printf("Записано %d записей в отчет", batchCount)
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return err
	}

	log.Printf("Всего записано %d записей в отчет", batchCount)
	return nil
}
