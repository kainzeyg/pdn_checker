package main

import (
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"strings"

	_ "github.com/denisenkom/go-mssqldb"
)

// Структуры данных
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
	FoundIn      string // "header" или "value"
	SampleValue  string
	Pattern      string
	PDNType      string
}

func main() {
	// Ввод параметров подключения
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

	// Подключение к БД
	connString := fmt.Sprintf("server=%s;port=%s;database=%s;user id=%s;password=%s",
		server, port, database, username, password)

	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatal("Ошибка подключения:", err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal("Ошибка проверки подключения:", err)
	}

	fmt.Println("Успешное подключение к БД")

	// Получаем список таблиц и представлений
	tables, err := getTablesAndViews(db)
	if err != nil {
		log.Fatal("Ошибка получения таблиц:", err)
	}

	var results []PDNResult

	// Анализ каждой таблицы
	for _, table := range tables {
		columns, err := getColumns(db, table.SchemaName, table.TableName)
		if err != nil {
			log.Printf("Ошибка получения колонок для %s.%s: %v", table.SchemaName, table.TableName, err)
			continue
		}

		for _, column := range columns {
			// Проверка НАЗВАНИЯ столбца
			if pdnTypes := checkForPDNPatterns(column.ColumnName); len(pdnTypes) > 0 {
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
			}

			// Проверка ЗНАЧЕНИЙ в столбце
			values, err := getSampleValues(db, table.SchemaName, table.TableName, column.ColumnName)
			if err != nil {
				log.Printf("Ошибка получения значений для %s.%s: %v", table.TableName, column.ColumnName, err)
				continue
			}

			for _, val := range values {
				if pdnTypes := checkForPDNPatterns(val.Value); len(pdnTypes) > 0 {
					for _, pdnType := range pdnTypes {
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
		}
	}

	// Вывод результатов
	printResults(results)
}

// Получение списка таблиц и представлений
func getTablesAndViews(db *sql.DB) ([]TableInfo, error) {
	query := `
		SELECT s.name AS schema_name, t.name AS table_name, t.type_desc AS table_type
		FROM sys.tables t
		INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
		UNION ALL
		SELECT s.name AS schema_name, v.name AS view_name, 'VIEW' AS table_type
		FROM sys.views v
		INNER JOIN sys.schemas s ON v.schema_id = s.schema_id
	`

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []TableInfo
	for rows.Next() {
		var ti TableInfo
		err := rows.Scan(&ti.SchemaName, &ti.TableName, &ti.TableType)
		if err != nil {
			return nil, err
		}
		tables = append(tables, ti)
	}

	return tables, nil
}

// Получение столбцов таблицы
func getColumns(db *sql.DB, schemaName, tableName string) ([]ColumnInfo, error) {
	query := `
		SELECT c.name AS column_name, tp.name AS data_type
		FROM sys.columns c
		JOIN sys.objects o ON c.object_id = o.object_id
		JOIN sys.schemas s ON o.schema_id = s.schema_id
		JOIN sys.types tp ON c.user_type_id = tp.user_type_id
		WHERE s.name = @schema AND o.name = @table
	`

	rows, err := db.Query(query, sql.Named("schema", schemaName), sql.Named("table", tableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var ci ColumnInfo
		err := rows.Scan(&ci.ColumnName, &ci.DataType)
		if err != nil {
			return nil, err
		}
		columns = append(columns, ci)
	}

	return columns, nil
}

// Получение примеров значений из столбца
func getSampleValues(db *sql.DB, schemaName, tableName, columnName string) ([]ValuePattern, error) {
	query := fmt.Sprintf(`
		SELECT TOP 50 CAST([%s] AS NVARCHAR(MAX)) AS sample_value
		FROM [%s].[%s]
		WHERE [%s] IS NOT NULL AND [%s] != ''
	`, columnName, schemaName, tableName, columnName, columnName)

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var values []string
	for rows.Next() {
		var val string
		err := rows.Scan(&val)
		if err != nil {
			return nil, err
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

// Определение паттерна значения
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

// Проверка на ПДн по расширенным правилам
func checkForPDNPatterns(input string) []string {
	input = strings.ToLower(input)
	var foundTypes []string

	// Паттерны для проверки значений
	valuePatterns := map[string]*regexp.Regexp{
		"Email":           regexp.MustCompile(`[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}`),
		"Телефон":         regexp.MustCompile(`(\+7|8)[\s\-\(]?\d{3}[\)\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}`),
		"Паспорт РФ":      regexp.MustCompile(`\b(\d{2}\s?\d{2}\s?\d{6}|\d{10})\b|(?:паспорт|серия|номер)[^\d]*(\d{4})[^\d]*(\d{6})`),
		"СНИЛС":           regexp.MustCompile(`\b\d{3}[-]?\d{3}[-]?\d{3}[-\s]?\d{2}\b`),
		"ИНН физлица":     regexp.MustCompile(`\b\d{12}\b`),
		"Кредитная карта": regexp.MustCompile(`\d{4}[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?\d{4}`),
	}

	// Паттерны для проверки заголовков
	headerPatterns := map[string][]string{
		"ФИО":                 {"фамил", "fami", "surn", "lastname", "last name", "имя", "name", "firstname", "first name", "отчест", "middlename", "middle name", "patronym", "фам", "fio", "фио", "fullname", "full name"},
		"Персональные данные": {"сотруд", "руковод", "manag", "физи", "персон", "person"},
		"Адрес":               {"адрес", "address", "addr", "location", "место"},
		"Email":               {"эп", "mail", "адресэп", "адрес эп", "email"},
		"Телефон":             {"телефон", "phone", "tel", "мобильн", "mobile", "contactno"},
		"Паспорт":             {"паспорт", "passport", "серия", "series", "номер", "number"},
		"СНИЛС/ИНН":           {"снилс", "snils", "инн", "taxid", "tax id"},
		"Дата рождения":       {"рожд", "birth", "dateofbirth", "birthdate", "датарожд", "дата рожд"},
		"Таб. номер":          {"таб", "табель"},
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

// Вспомогательные функции
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

func appendIfNotExists(slice []string, item string) []string {
	for _, s := range slice {
		if s == item {
			return slice
		}
	}
	return append(slice, item)
}

// Вывод результатов
func printResults(results []PDNResult) {
	fmt.Println("\nРезультаты поиска ПДн:")
	fmt.Println("==============================================")

	if len(results) == 0 {
		fmt.Println("ПДн не обнаружено")
		return
	}

	for _, res := range results {
		fmt.Printf("БД: %s\n", res.DatabaseName)
		fmt.Printf("Схема: %s\n", res.SchemaName)
		fmt.Printf("Таблица/Представление: %s (%s)\n", res.TableName, res.TableType)
		fmt.Printf("Столбец: %s\n", res.ColumnName)
		fmt.Printf("Обнаружено в: %s\n", res.FoundIn)
		if res.FoundIn == "value" {
			fmt.Printf("Пример значения: %s\n", res.SampleValue)
			fmt.Printf("Паттерн значения: %s\n", res.Pattern)
		}
		fmt.Printf("Тип ПДн: %s\n", res.PDNType)
		fmt.Println("----------------------------------------------")
	}
}
