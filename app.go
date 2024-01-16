package main

import (
	"database/sql"
	"fmt"
	_ "github.com/sijms/go-ora/v2"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"oracleSeq2pg/connect"
	"time"
)

var log = logrus.New()
var srcDb *sql.DB

func initConfig() {
	var cfgFile string
	cfgFile = "dbcfg.yaml"
	viper.SetConfigFile(cfgFile)
	// 通过viper读取配置文件进行加载
	if err := viper.ReadInConfig(); err == nil {
		log.Info("Using config file:", viper.ConfigFileUsed())
	} else {
		log.Fatal(viper.ConfigFileUsed(), " has some error please check your yml file ! ", "Detail-> ", err)
	}
	log.Info("Using config file:", cfgFile)
}

func getConn() (connStr *connect.DbConnStr) {
	connStr = new(connect.DbConnStr)
	connStr.SrcHost = viper.GetString("src.host")
	connStr.SrcUserName = viper.GetString("src.username")
	connStr.SrcPassword = viper.GetString("src.password")
	connStr.SrcDatabase = viper.GetString("src.database")
	connStr.SrcPort = viper.GetInt("src.port")
	connStr.DestHost = viper.GetString("dest.host")
	connStr.DestPort = viper.GetInt("dest.port")
	connStr.DestUserName = viper.GetString("dest.username")
	connStr.DestPassword = viper.GetString("dest.password")
	connStr.DestDatabase = viper.GetString("dest.database")
	return connStr
}

func PrepareSrc(connStr *connect.DbConnStr) {
	// 生成源库连接
	srcHost := connStr.SrcHost
	srcUserName := connStr.SrcUserName
	srcPassword := connStr.SrcPassword
	srcDatabase := connStr.SrcDatabase
	srcPort := connStr.SrcPort
	srcConn := fmt.Sprintf("oracle://%s:%s@%s:%d/%s?LOB FETCH=POST", srcUserName, srcPassword, srcHost, srcPort, srcDatabase)
	//fmt.Println(srcConn)
	var err error
	srcDb, err = sql.Open("oracle", srcConn) //go-ora
	// 以下是需要oracle instant client依赖的库godror
	//srcDb, err = sql.Open("godror", `user="one" password="oracle" connectString="192.168.189.200:1521/orcl" libDir="/Users/kay/Documents/database/oracle/instantclient_19_8_mac"`)//直接连接方式
	//oracleConnStr.LibDir = "instantclient"
	//oracleConnStr.Username = srcUserName
	//oracleConnStr.Password = godror.NewPassword(srcPassword)
	//oracleConnStr.ConnectString = fmt.Sprintf("%s:%s/%s", srcHost, strconv.Itoa(srcPort), srcDatabase)
	//srcDb = sql.OpenDB(godror.NewConnector(oracleConnStr))
	if err != nil {
		log.Fatal("please check SourceDB yml file", err)
	}
	c := srcDb.Ping()
	if c != nil {
		log.Fatal("connect Source database failed ", c)
	}
	srcDb.SetConnMaxLifetime(2 * time.Hour) // 一个连接被使用的最长时间，过一段时间之后会被强制回收
	srcDb.SetMaxIdleConns(0)                // 最大空闲连接数，0为不限制
	srcDb.SetMaxOpenConns(0)                // 设置连接池最大连接数
	log.Info("connect Source ", srcHost, " success")
}
