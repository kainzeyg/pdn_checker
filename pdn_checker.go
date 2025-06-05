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
	// Ввод параметров подключения
	server, port, database, username, password := getConnectionParams()

	// Подключение к БД с таймаутами
	db := connectToDB(server, port, database, username, password)
	defer db.Close()

	// Получаем список таблиц и представлений
	tables := getTablesAndViews(db)
	fmt.Printf("\nНайдено %d таблиц/представлений для анализа\n", len(tables))

	// Анализ таблиц
	results := analyzeTables(db, database, tables)

	// Вывод результатов в CSV
	err := saveResultsToCSV(server, results)
	if err != nil {
		log.Fatal("Ошибка сохранения в CSV:", err)
	}
	fmt.Println("\nОтчет успешно сохранен в report.csv")
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

	// Настройки соединения
	db.SetConnMaxLifetime(10 * time.Minute)
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)

	// Проверка подключения с таймаутом
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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

func analyzeTables(db *sql.DB, database string, tables []TableInfo) []PDNResult {
	var results []PDNResult
	totalTables := len(tables)

	for i, table := range tables {
		fmt.Printf("\n[%d/%d] Анализ %s.%s (%s)...\n",
			i+1, totalTables, table.SchemaName, table.TableName, table.TableType)

		columns, err := getColumns(db, table.SchemaName, table.TableName)
		if err != nil {
			log.Printf("⚠ Ошибка получения колонок: %v - пропускаем\n", err)
			continue
		}

		fmt.Printf("  Найдено %d колонок\n", len(columns))

		for _, column := range columns {
			columnResults := analyzeColumn(db, database, table, column)
			results = append(results, columnResults...)
		}
	}

	// Добавляем записи для колонок без ПДн
	for _, table := range tables {
		columns, err := getColumns(db, table.SchemaName, table.TableName)
		if err != nil {
			continue
		}

		for _, column := range columns {
			hasPDN := false
			for _, res := range results {
				if res.SchemaName == table.SchemaName && res.TableName == table.TableName && res.ColumnName == column.ColumnName {
					hasPDN = true
					break
				}
			}

			if !hasPDN {
				results = append(results, PDNResult{
					DatabaseName: database,
					SchemaName:   table.SchemaName,
					TableName:    table.TableName,
					TableType:    table.TableType,
					ColumnName:   column.ColumnName,
					FoundIn:      "none",
					SampleValue:  "",
					Pattern:      "",
					PDNType:      "Нет",
				})
			}
		}
	}

	return results
}

func getColumns(db *sql.DB, schemaName, tableName string) ([]ColumnInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

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

func analyzeColumn(db *sql.DB, database string, table TableInfo, column ColumnInfo) []PDNResult {
	var results []PDNResult
	fmt.Printf("    Колонка %s (%s)... ", column.ColumnName, column.DataType)

	// Проверка названия колонки
	pdnTypes := checkForPDNPatterns(column.ColumnName)
	if len(pdnTypes) > 0 {
		for _, pdnType := range pdnTypes {
			results = append(results, PDNResult{
				DatabaseName: database,
				SchemaName:   table.SchemaName,
				TableName:    table.TableName,
				TableType:    table.TableType,
				ColumnName:   column.ColumnName,
				FoundIn:      "header",
				SampleValue:  "N/A",
				Pattern:      "",
				PDNType:      pdnType,
			})
		}
		fmt.Printf("ПДн в названии (%s) ", strings.Join(pdnTypes, ", "))
	}

	// Проверка значений
	values, err := getSampleValues(db, table.SchemaName, table.TableName, column.ColumnName)
	if err != nil {
		fmt.Printf("ошибка значений: %v\n", err)
		return results
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
					SampleValue:  maskSensitiveData(val.Value),
					Pattern:      val.Pattern,
					PDNType:      pdnType,
				})
			}
		}
	}

	if len(valuePdnTypes) > 0 {
		fmt.Printf("ПДн в значениях (%s)", strings.Join(valuePdnTypes, ", "))
	}

	if len(pdnTypes) == 0 && len(valuePdnTypes) == 0 {
		fmt.Printf("нет ПДн")
	}

	fmt.Println()
	return results
}

func getSampleValues(db *sql.DB, schemaName, tableName, columnName string) ([]ValuePattern, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	query := fmt.Sprintf(`
		SELECT TOP 20 CAST([%s] AS NVARCHAR(MAX)) AS sample_value
		FROM [%s].[%s] WITH (NOLOCK)
		WHERE [%s] IS NOT NULL AND [%s] != ''
	`, columnName, schemaName, tableName, columnName, columnName)

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("запрос значений: %v", err)
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

	// Группировка по паттернам
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

func checkForPDNPatterns(input string) []string {
	input = strings.ToLower(input)
	var foundTypes []string

	// Паттерны значений
	valuePatterns := map[string]*regexp.Regexp{
		"Email":           regexp.MustCompile(`[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}`),
		"Телефон":         regexp.MustCompile(`(\+7|8)[\s\-\(]?\d{3}[\)\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}`),
		"Паспорт РФ":      regexp.MustCompile(`\b(\d{2}\s?\d{2}\s?\d{6}|\d{10})\b|(?:паспорт|серия|номер)[^\d]*(\d{4})[^\d]*(\d{6})`),
		"СНИЛС":           regexp.MustCompile(`\b\d{3}[-]?\d{3}[-]?\d{3}[-\s]?\d{2}\b`),
		"ИНН физлица":     regexp.MustCompile(`\b\d{12}\b`),
		"Кредитная карта": regexp.MustCompile(`\d{4}[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?\d{4}`),
	}

	// Паттерны заголовков
	headerPatterns := map[string][]string{
		"ФИО":                 {"фамил", "fami", "surn", "lastname", "last name", "имя", "name", "firstname", "first name", "отчест", "middlename", "middle name", "patronym", "фам", "fio", "фио", "fullname", "full name"},
		"Персональные данные": {"сотруд", "руковод", "manag", "физи", "персон", "person", "empl"},
		"Адрес":               {"адрес", "address", "addr", "location", "место"},
		"Email":               {"эп", "mail", "адресэп", "адрес эп", "email"},
		"Телефон":             {"телефон", "phone", "tel", "мобильн", "mobile", "contactno"},
		"Паспорт":             {"паспорт", "passport", "серия", "series", "номер", "number"},
		"СНИЛС/ИНН":           {"снилс", "snils", "инн", "taxid", "tax id"},
		"Дата рождения":       {"рожд", "birth", "dateofbirth", "birthdate", "датарожд", "дата рожд"},
		"Таб. номер":          {"таб", "табель"},
		"Фото":                {"фото", "foto", "photo"},
	}

	// Проверка значений
	for pdnType, re := range valuePatterns {
		if re.MatchString(input) {
			foundTypes = append(foundTypes, pdnType)
		}
	}

	// Проверка заголовков
	for pdnType, keywords := range headerPatterns {
		for _, keyword := range keywords {
			if strings.Contains(input, keyword) {
				foundTypes = appendIfNotExists(foundTypes, pdnType)
			}
		}
	}

	// Дополнительные проверки
	if containsAny(input, []string{"ул.", "улица", "дом", "кв.", "квартира"}) {
		foundTypes = appendIfNotExists(foundTypes, "Адрес")
	}

	if containsAny(input, []string{"др", "дата рождения", "birthday"}) {
		foundTypes = appendIfNotExists(foundTypes, "Дата рождения")
	}

	return foundTypes
}

func maskSensitiveData(value string) string {
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

func saveResultsToCSV(server string, results []PDNResult) error {
	file, err := os.Create("report.csv")
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Записываем заголовок
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

	// Записываем данные
	for _, res := range results {
		hasPDN := "Да"
		if res.PDNType == "Нет" {
			hasPDN = "Нет"
		}

		record := []string{
			server,
			res.DatabaseName,
			res.SchemaName,
			res.TableName,
			res.TableType,
			res.ColumnName,
			hasPDN,
			res.PDNType,
			res.SampleValue,
			maskSensitiveData(res.SampleValue),
		}

		if err := writer.Write(record); err != nil {
			return err
		}
	}

	return nil
}
