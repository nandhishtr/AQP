#define NOMINMAX
#define _NO_CRT_STDIO_INLINE
#pragma comment(linker, "/STACK:3000000000") //for batch_read in precomputation
#pragma comment(linker, "/HEAP:3000000000") //for batch_read in precomputation

#include<stdio.h>
#include<thread>
#include <string>
#include <iostream>
#include<cstdio>
#include<random>
#include<map>
#include<unordered_map>
#include<Windows.h>
#include<sqlext.h>
#include <exception>
#include"aqpp\configuration.h"
#include"aqpp\sql_interface.h"
#include "exp\comprehensive_exp.h"
#include<any>
#include<vector>
#include<sql.h>
#include<unordered_map>
#include"aqpp\common_content.h"

using namespace std;
using namespace aqppp;

void ShowError(unsigned int handletype, const SQLHANDLE& handle)
{
	//	char* sqlstate;
	SQLCHAR sqlstate[1024] = {};
	SQLCHAR message[1024] = {};
	if (SQL_SUCCESS == SQLGetDiagRec(handletype, handle, 1, sqlstate, NULL, message, 1024, NULL)) {

		std::wcout << "Message: " << ((const char*)message) << "\nSQLSTATE: " << (const char*)sqlstate << std::endl;
	}
}


int GenQuery(SQLHANDLE& sqlconnectionhandle)
{
	int QUERY_NUM = 1000;
	double min_sel = 0.005;
	double max_sel = 0.05;
	std::vector<std::vector<double>> sample = std::vector<std::vector<double>>();
	//std::vector<std::vector<double>> sample = std::vector<std::vector<double>>();
	aqppp::Settings PAR = aqppp::Settings();
	PAR.DB_NAME = "skew_s100_z2.dbo";
	PAR.CONDITION_NAMES = { "L_ORDERKEY","L_PARTKEY" };
	aqppp::SqlInterface::CreateDbSamples(sqlconnectionhandle, PAR.RAND_SEED, PAR.DB_NAME, PAR.TABLE_NAME, { PAR.SAMPLE_RATE,PAR.SUB_SAMPLE_RATE }, { PAR.SAMPLE_NAME,PAR.SUB_SAMPLE_NAME });
	sample = aqppp::Tool::loadDataFromFile("cache\\readSamples.txt");
	if (sample.empty())
	{
		expDemo::ReadSamples(sqlconnectionhandle, PAR, 1, sample, std::vector<std::vector<double>>());
		aqppp::Tool::saveDataToFile("cache\\readSamples.txt", sample);
	}
	//expDemo::ReadBLBSamples(sqlconnectionhandle, PAR, 50, std::vector <std::vector <std::vector<double>>>());
	std::vector<std::vector<aqppp::CA>> CAsample = std::vector<std::vector<aqppp::CA>>();
	CAsample = aqppp::Tool::loadCASampleDataFromFile("cache\\TransSample.txt");
	if (CAsample.empty())
	{
		aqppp::Tool::TransSample(sample, CAsample);
		aqppp::Tool::saveDataToFile("cache\\TransSample.txt", CAsample);
	}
	std::vector<std::vector<aqppp::Condition>> user_queries = std::vector<std::vector<aqppp::Condition>>();
	aqppp::Tool::ReadQueriesFromFile("cache\\default_queries.txt", QUERY_NUM, user_queries);
	if (user_queries.empty())
	{
		aqppp::Tool::GenUserQuires(sample, CAsample, PAR.RAND_SEED, QUERY_NUM, { min_sel,max_sel }, user_queries);
		aqppp::Tool::SaveQueryFile("cache\\default_queries.txt", user_queries);
		// user_queries format: lb0 ub0 lb1 ub1
		// user_queries format: lb0 ub0 lb1 ub1
	}

	return 0;
}




int main()
{

	//auto conf = aqppp::Configuration().ReadConfig("C:/Users/Burtsev/Desktop/aqppp-master/aqppp2/settings.txt");
	//std::any t = std::make_any<std::string>("12");
	//std::cout<<"convert any:"<<std::any_cast<std::string>(t);



	srand(0);
	std::cout << " --------------------------------------------------------------------------------------------------- " << std::endl;
	std::cout << " The parameters are set at structure 'Settings' in comment_content.h. This code was written using visual studio 2017, and run in Windows System. " << std::endl;
	std::cout << "It uses ODBC to connect SQL Server 2017. The connection string could be found in the main function(you should set or create related account and ODBC). Please change the connection string based on your setting. " << std::endl;
	std::cout << "Besides, please load lineitem table into SQLServer, using the name in parameter. For example, if you set par.DB_NAME = 'skew_s100_z2.dbo', and par.TABLE_NAME='lineitem', then crease a database called skew_s100_z2, and a table called lineitem in SQLServer. " << std::endl;
	std::cout << "Then load lineitem.tbl (generated using TPCD benchmark by command -s 100 -z 2 -T L) into the lineitem table in SQLServer. Then the code should work. The results will be saved on exp_result folder." << std::endl;
	std::cout << " --------------------------------------------------------------------------------------------------- " << std::endl;
	std::cout << "To run the demo, input 1" << std::endl;
	std::cout << "2--query generation" << std::endl;

	SQLHANDLE sqlenvhandle = NULL;
	SQLHANDLE sqlconnectionhandle = NULL;
	SQLHANDLE sqlstatementhandle = NULL;
	if (SQL_SUCCESS != SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &sqlenvhandle)) return -1;
	if (SQL_SUCCESS != SQLSetEnvAttr(sqlenvhandle, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0)) return -1;
	if (SQL_SUCCESS != SQLAllocHandle(SQL_HANDLE_DBC, sqlenvhandle, &sqlconnectionhandle)) return -1;
	ShowError(SQL_HANDLE_DBC, sqlconnectionhandle);
	aqppp::ConnectionSettings CONNECTDATA = aqppp::ConnectionSettings();
	SQLCHAR* serverName = (SQLCHAR*)(CONNECTDATA.SERVER_NAME).c_str();
	SQLCHAR* userName = (CONNECTDATA.USER_NAME).empty() ? NULL : (SQLCHAR*)(CONNECTDATA.USER_NAME).c_str();
	SQLCHAR* password = (CONNECTDATA.PASSWORD).empty() ? NULL : (SQLCHAR*)(CONNECTDATA.PASSWORD).c_str();
	SQLCHAR* connectionString = (SQLCHAR*)(CONNECTDATA.CONNECTION_STRING).c_str();
	switch (SQLDriverConnect(sqlconnectionhandle, NULL, connectionString, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT)) 
	{
	case SQL_SUCCESS_WITH_INFO:
		std::cout << "success" << std::endl;
		ShowError(SQL_HANDLE_DBC, sqlconnectionhandle);
		break;
	default:
		std::cout << "error" << std::endl;
		ShowError(SQL_HANDLE_DBC, sqlconnectionhandle);
		getchar();
		return -1;
	}

	std::string user_choices;
	std::cin >> user_choices;
	std::vector<std::string> exp_ids = aqppp::Tool::split(user_choices, ',');
	for (std::string st : exp_ids)
	{
		try {
			if (st == "1" || st == "all") expDemo::ComprehensiveExp::Exp(sqlconnectionhandle);
			if (st == "2" || st == "all") GenQuery(sqlconnectionhandle);
		}
		catch (std::exception& e) {
			//ShowError(SQL_HANDLE_DBC, sqlconnectionhandle);
			std::cout << "some error occurs " << e.what() << std::endl;

			getchar();
		}
	}

	std::cout << "all exp end" << std::endl;
	SQLDisconnect(sqlconnectionhandle);
	SQLFreeHandle(SQL_HANDLE_DBC, sqlconnectionhandle);
	SQLFreeHandle(SQL_HANDLE_ENV, sqlenvhandle);
	getchar();
	return 0;

}


