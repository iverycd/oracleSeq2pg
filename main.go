package main

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"io"
	"os"
	"regexp"
	"strings"
	"time"
)

func SeqCreate() {
	startTime := time.Now()
	var dbRet, tableName string
	rows, err := srcDb.Query("select lower(table_name),trigger_body from user_triggers where upper(trigger_type) ='BEFORE EACH ROW'")
	if err != nil {
		log.Error(err)
	}
	defer rows.Close()
	idx := 0
	fmt.Println("Oracle sequence")
	for rows.Next() {
		idx += 1
		err := rows.Scan(&tableName, &dbRet)
		if err != nil {
			log.Error(err)
		}
		dbRet = strings.ToUpper(dbRet)
		dbRet = strings.ReplaceAll(dbRet, "INTO:", "INTO :")
		dbRet = strings.ReplaceAll(dbRet, "SYS.DUAL ", "DUAL")
		dbRet = strings.ReplaceAll(dbRet, "SYS.DUAL", "DUAL")
		dbRet = strings.ReplaceAll(dbRet, "\n", "")
		pattern := `SELECT\s+(.*?)\.NEXTVAL\s+INTO\s+:NEW\.`
		re := regexp.MustCompile(pattern)
		match := re.FindStringSubmatch(dbRet)
		if len(match) > 0 { // 第一层，先正则匹配SELECT .NEXTVAL INTO :NEW包含的字符窜,主要是要匹配到自增列性质的触发器
			//如果符合第一层正则的条件，再匹配第二层，第二层主要是获取:NEW.后面的名称，即自增列名称
			re := regexp.MustCompile(`:NEW\.(\w+)`) // 正则表达式，匹配以 ":NEW." 开头的字符串，并提取后面的单词字符（包括字母、数字和下划线）
			match := re.FindStringSubmatch(dbRet)   // 查找匹配项
			if len(match) == 2 {
				autoColName := match[1]
				autoColName = strings.ToLower(autoColName)
				// 生成pg类型的序列拼接sql
				sqlModifyAuto := fmt.Sprintf("alter table "+tableName+" alter column "+autoColName+" set default nextval('sq_%s');", tableName)
				log.Printf(sqlModifyAuto)
			}
		}
	}
	cost := time.Since(startTime)
	fmt.Println("cost time: ", cost)
}

func main() {
	// 初始化配置文件
	initConfig()
	// 初始化获取连接字符串
	connStr := getConn()
	// 输出调用文件以及方法位置
	log.SetReportCaller(false)
	// log格式化
	formatter := &logrus.TextFormatter{
		// 不需要彩色日志
		DisableColors:          true,
		DisableTimestamp:       true,
		DisableLevelTruncation: true,
		DisableQuote:           true,
		DisableSorting:         true,
		//FieldMap: logrus.FieldMap{
		//	logrus.FieldKeyLevel: "",
		//	logrus.FieldKeyMsg:   " ",
		//	logrus.FieldKeyFunc:  "@caller",
		//},
	}
	log.SetFormatter(formatter)
	// 运行日志
	f, err := os.OpenFile("run.txt", os.O_CREATE|os.O_TRUNC|os.O_RDWR, os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Fatal(err)
		}
	}()
	// log信息重定向到平面文件
	multiWriter := io.MultiWriter(os.Stdout, f)
	log.SetOutput(multiWriter)
	// 生成源库数据库连接
	PrepareSrc(connStr)
	defer srcDb.Close()
	// 输出序列拼接sql
	SeqCreate()
	// 读取文件内容
	content, err := os.ReadFile("run.txt")
	if err != nil {
		panic(err)
	}
	// 去掉包含"level=info msg="的字符
	filteredContent := strings.ReplaceAll(string(content), "level=info msg=", "")
	// 将过滤后的内容写回文件
	err = os.WriteFile("sql.txt", []byte(filteredContent), 0644)
	if err != nil {
		panic(err)
	}
}
