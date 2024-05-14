//#include"stdafx.h"
#include"tool.h"
#include"sql_interface.h"
#include<iostream>
namespace aqppp {

	void SqlInterface::ShowError(unsigned int handletype, const SQLHANDLE& handle)
	{
		SQLCHAR sqlstate[1024];
		SQLCHAR message[1024];
		SQLSMALLINT actual_msg_len = 0;
		SQLGetDiagRec(handletype, handle, 1, sqlstate, NULL, message, 1024, &actual_msg_len);

		std::wcout << "Message: ";
		for (int i = 0; i < actual_msg_len; i++)
		{
			std::wcout << static_cast<wchar_t>(message[i]);
		}

		std::wcout << "\nSQLSTATE: ";
		for (int i = 0; i < SQL_SQLSTATE_SIZE; i++)
		{
			std::wcout << static_cast<wchar_t>(sqlstate[i]);
		}

		std::wcout << std::endl;
	}



	void SqlInterface::MakeSqlConnection(std::string odbc_name, std::string user_name, std::string pwd, SQLHANDLE& sqlconnectionhandle)
	{
		SQLHANDLE sqlenvhandle = NULL;
		if (SQL_SUCCESS != SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &sqlenvhandle)) return;
		if (SQL_SUCCESS != SQLSetEnvAttr(sqlenvhandle, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0)) return;
		if (SQL_SUCCESS != SQLAllocHandle(SQL_HANDLE_DBC, sqlenvhandle, &sqlconnectionhandle)) return;
		SQLCHAR* serverName = (SQLCHAR*)odbc_name.c_str();
		SQLCHAR* username = user_name.empty() ? NULL : (SQLCHAR*)user_name.c_str();
		SQLCHAR* password = pwd.empty() ? NULL : (SQLCHAR*)pwd.c_str();
		switch (SQLConnect(sqlconnectionhandle, serverName, SQL_NTS, username, SQL_NTS, password, SQL_NTS))
		{
		case SQL_SUCCESS_WITH_INFO:
			std::cout << "success" << std::endl;
			ShowError(SQL_HANDLE_DBC, sqlconnectionhandle);
			break;
		default:
			std::cout << "error" << std::endl;
			ShowError(SQL_HANDLE_DBC, sqlconnectionhandle);
			getchar();
			return;
		}
	}


	int SqlInterface::ConnectDb(SQLHANDLE& sqlconnectionhandle, std::string dsn, std::string user, std::string pwd)
	{
		int retcode = 0;
		switch (SQLConnect(sqlconnectionhandle, (SQLCHAR*)dsn.c_str(), SQL_NTS, (SQLCHAR*)user.c_str(), SQL_NTS, (SQLCHAR*)pwd.c_str(), SQL_NTS))
		{
		case SQL_SUCCESS_WITH_INFO:
			std::cout << "success" << std::endl;
			ShowError(SQL_HANDLE_DBC, sqlconnectionhandle);
			break;
		case SQL_INVALID_HANDLE:
		case SQL_ERROR:
			ShowError(SQL_HANDLE_DBC, sqlconnectionhandle);
			retcode = -1;
			break;
		default:
			ShowError(SQL_HANDLE_DBC, sqlconnectionhandle);
			break;
		}
		return retcode;
	}


	/*
	return a query result of given string query.
	*/
	void SqlInterface::SqlQuery(std::string query, SQLHANDLE& sqlstatementhandle)
	{
		// Convert the std::string query to a wide string (std::wstring)
		
		std::wstring wquery(query.begin(), query.end());
		wquery.push_back(L'\0');
		// Convert the wide string to a narrow string (std::string)
		
		int size_needed = WideCharToMultiByte(CP_UTF8, 0, &wquery[0], static_cast<int>(wquery.size()), NULL, 0, NULL, NULL);
		std::vector<char> narrow_query(size_needed,0);
		narrow_query.resize(size_needed);
		WideCharToMultiByte(CP_UTF8, 0, &wquery[0], static_cast<int>(wquery.size()), &narrow_query[0], size_needed, NULL, NULL);
		// Execute the SQL query using SQLExecDirect
		std::cout << "success"<<SQL_SUCCESS;
		std::cout << "exec"<<SQLExecDirect<<std::endl;
		//if (SQL_SUCCESS != SQLExecDirect(sqlstatementhandle, reinterpret_cast<SQLCHAR*>(&narrow_query[0]), SQL_NTS))
		//{
			//std::cout << "executing this error" << std::endl;
			switch (SQLExecDirect(sqlstatementhandle, reinterpret_cast<SQLCHAR*>(&narrow_query[0]), SQL_NTS))
			{
			case SQL_SUCCESS:
				std::cout << "Success" << std::endl;
				break;
			case SQL_NO_DATA:
				std::cout << "no data" << std::endl;
				break;
			case SQL_INVALID_HANDLE:
				std::cout << "invalid" << std::endl;
				break;
			case SQL_STILL_EXECUTING:
				std::cout << "still exec" << std::endl;
			}
			ShowError(SQL_HANDLE_STMT, sqlstatementhandle);
		//}
	}


	/*create sample and small_sample table in MySQL database.
	*/
	std::pair<double, double> SqlInterface::CreateDbSamples(SQLHANDLE &sqlconnectionhandle, int seed, std::string db_name, std::string table_name, std::pair<double, double> sample_rates, std::pair<std::string, std::string> sample_names, std::vector<std::string> num_dim4rand, std::vector<std::string> ctg_dim4rand)
	{
		double t1=CreateDbSample(sqlconnectionhandle, seed+1, db_name, table_name,sample_rates.first, sample_names.first, num_dim4rand, ctg_dim4rand);
		double t2= CreateDbSample(sqlconnectionhandle, seed+2, db_name, sample_names.first, sample_rates.second, sample_names.second, num_dim4rand, ctg_dim4rand);
		//double t3 = CreateBLBDbSample(sqlconnectionhandle, seed+1, db_name, table_name, sample_rates.second, sample_names.second, num_dim4rand, ctg_dim4rand);
		std::cout << __func__ << " Time taken sample: " << t1 << std::endl;
		std::cout << __func__ << " Time taken small sample: " << t2 << std::endl;
		return { t1,t2 };
	}

	std::string SqlInterface::ComputeRandStr(int seed, double sample_rate, std::vector<std::string> num_dim4rand, std::vector<std::string> ctg_dim4rand)
	{
		if (num_dim4rand.size() == 0 && ctg_dim4rand.size() == 0)
		{
			num_dim4rand = { "L_ORDERKEY","L_LINENUMBER","L_PARTKEY","L_QUANTITY","L_EXTENDEDPRICE","L_DISCOUNT","L_TAX"};
			ctg_dim4rand = { "L_COMMENT","L_RETURNFLAG","L_SHIPDATE","L_COMMITDATE","L_RECEIPTDATE","L_SHIPINSTRUCT","L_SHIPMODE" };
		}
		int tpseed = seed + 1;
		std::string numeric_part = std::to_string(seed);
		std::string catagory_part = "";
		for (auto st : num_dim4rand)
		{
			numeric_part += ", " + std::to_string(tpseed) + "*" + st;
			tpseed++;
		}
		std::cout << "numeric part" << numeric_part;
		for (auto st : ctg_dim4rand)
		{
			catagory_part += ", "+st;
		}
		return "RAND(BINARY_CHECKSUM(" + numeric_part + catagory_part+"))<=" + std::to_string(sample_rate);
	}

	double SqlInterface::CreateDbSample(SQLHANDLE &sqlconnectionhandle, int seed, std::string db_name, std::string table_name, double sample_rate, std::string sample_name, std::vector<std::string> num_dim4rand, std::vector<std::string> ctg_dim4rand)
	{
		SQLHANDLE sqlstatementhandle = NULL;
		if (SQLAllocHandle(SQL_HANDLE_STMT, sqlconnectionhandle, &sqlstatementhandle) != SQL_SUCCESS) return -1;
		std::string sample_full_name = db_name + "." + sample_name;
		std::string table_full_name = db_name + "." + table_name;
		std::string drop_sample = "IF OBJECT_ID('" + sample_full_name + "', 'U') IS NOT NULL DROP TABLE " + sample_full_name + "; ";
		std::string create_sample = "SELECT *  INTO "+sample_full_name + " FROM " + table_full_name + " WHERE " + ComputeRandStr(seed,sample_rate, num_dim4rand, ctg_dim4rand)+";";
		std::string create_sample_cstore_indx = "CREATE CLUSTERED COLUMNSTORE INDEX cci_"+sample_name+" ON " + sample_full_name + ";";
		std::cout << drop_sample << std::endl;
		std::cout << create_sample << std::endl;
		std::cout << create_sample_cstore_indx << std::endl;
		// Check if the sample already exists
		//std::string check_sample_existence = "IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + db_name + "' AND TABLE_NAME = '" + sample_name + "') SELECT 1 ELSE SELECT 0;";
		//int sample_exists = 0;
		//SqlQuery(check_sample_existence, sqlstatementhandle, sample_exists);
		//SqlQuery(drop_sample, sqlstatementhandle);
		double t1 = clock();
		SqlQuery(create_sample, sqlstatementhandle);
		SqlQuery(create_sample_cstore_indx, sqlstatementhandle);
		double create_sample_time = (clock() - t1) / CLOCKS_PER_SEC;
		SQLFreeHandle(SQL_HANDLE_STMT, sqlstatementhandle);
		return create_sample_time;
	}

	double SqlInterface::CreateBLBDbSample(SQLHANDLE& sqlconnectionhandle, int seed, std::string db_name, std::string table_name, double sample_rate, std::string sample_name, std::vector<std::string> num_dim4rand, std::vector<std::string> ctg_dim4rand)
	{
		SQLHANDLE sqlstatementhandle = NULL;
		if (SQLAllocHandle(SQL_HANDLE_STMT, sqlconnectionhandle, &sqlstatementhandle) != SQL_SUCCESS) return -1;
		std::string sample_full_name = db_name + "." + sample_name;
		std::string table_full_name = db_name + "." + table_name;
		std::string drop_sample = "IF OBJECT_ID('" + sample_full_name + "', 'U') IS NOT NULL DROP TABLE " + sample_full_name + "; ";
		std::string create_sample = "SELECT *  INTO " + sample_full_name + " FROM " + table_full_name + " WHERE " + ComputeRandStr(seed, sample_rate, num_dim4rand, ctg_dim4rand) + ";";
		std::string create_sample_cstore_indx = "CREATE CLUSTERED COLUMNSTORE INDEX cci_" + sample_name + " ON " + sample_full_name + ";";
		std::cout << drop_sample << std::endl;
		std::cout << create_sample << std::endl;
		std::cout << create_sample_cstore_indx << std::endl;

		SqlQuery(drop_sample, sqlstatementhandle);
		double t1 = clock();
		SqlQuery(create_sample, sqlstatementhandle);
		SqlQuery(create_sample_cstore_indx, sqlstatementhandle);
		double create_sample_time = (clock() - t1) / CLOCKS_PER_SEC;
		SQLFreeHandle(SQL_HANDLE_STMT, sqlstatementhandle);
		return create_sample_time;
	}


	double SqlInterface::Column2Numeric(SQLHANDLE &sqlstatementhandle, int col_id, std::string col_name)
	{
		const int max_char_len = 300;
		if (col_name == "L_SHIPDATE" || col_name == "L_COMMITDATE" || col_name == "L_RECEIPTDATE" || col_name=="visitDate" || col_name=="pickup_date" || col_name=="dropoff_date")
		{
			double data = -1;
			TIMESTAMP_STRUCT ts;
			SQLGetData(sqlstatementhandle, col_id + 1, SQL_C_DATE, &ts, 0, NULL);
			data = ts.year * 10000 + ts.month * 100 + ts.day;
			return data;
		}
		if (col_name == "vendor_name")
		{
			double data = 0;
			char ts[10] = {};
			SQLGetData(sqlstatementhandle, col_id + 1, SQL_C_CHAR, ts, 10, NULL);
			std::string name = ts;
			if (name == "CMT") return 1;
			if (name == "DDS") return 2;
			if (name == "VTS") return 3;
			return -1;
		}
		if (col_name == "pickup_time" || col_name == "dropoff_time")
		{
			double data = -1;
			TIMESTAMP_STRUCT ts;
			SQLGetData(sqlstatementhandle, col_id + 1, SQL_C_TYPE_TIMESTAMP, &ts, 0, NULL);
			data = ts.hour * 10000 + ts.minute * 100 + ts.second;
			return data;
		}
		
		double data = 0;
		SQLGetData(sqlstatementhandle, col_id + 1, SQL_C_DOUBLE, &data, 0, NULL);
		/*if (col_name=="sourceIP")
		{
			char data[max_char_len] = {};
			SQLGetData(sqlstatementhandle, col_id + 1, SQL_C_CHAR, data, 0, NULL);
			//vector<string> ip_addr = Tool::split(string(data),'.');


		}
		*/

		return data;
	}
	

	std::string SqlInterface::ReadTableStr(std::string db_name, std::string table_name, std::string AGGREGATE_NAME, std::vector<std::string> CONDITION_NAMES)
	{
		std::string demand_str = AGGREGATE_NAME;
		for (int i = 0; i < CONDITION_NAMES.size(); i++)
			demand_str += ", " + CONDITION_NAMES[i];

		std::string query = "SELECT " + demand_str + " FROM " + db_name + "." + table_name + ";";
		return query;
	}

	double SqlInterface::ReadDb(SQLHANDLE &sqlconnectionhandle, std::vector<std::vector<double>> &o_table, std::string db_name, std::string table_name, std::string AGGREGATE_NAME, std::vector<std::string> CONDITION_NAMES)
	{
		SQLHANDLE sqlstatementhandle = NULL;
		if (SQLAllocHandle(SQL_HANDLE_STMT, sqlconnectionhandle, &sqlstatementhandle) != SQL_SUCCESS) return -1;

		double st = clock();
		o_table = std::vector<std::vector<double>>();
		table_name = "lineitem";
		std::string query = ReadTableStr(db_name, table_name, AGGREGATE_NAME, CONDITION_NAMES);
		SqlInterface::SqlQuery(query, sqlstatementhandle);
		short int COL_NUM = 0;
		// count the number of columns resulting from the query
		SQLNumResultCols(sqlstatementhandle, &COL_NUM);
		std::cout << "colnum " << COL_NUM;
		for (int i = 0; i < COL_NUM; i++)
			o_table.push_back(std::vector<double>());
			std::cout << " otable[ci] " << o_table[0].size()<<std::endl;
			switch (SQLFetch(sqlstatementhandle))
			{
			case SQL_SUCCESS:
				std::cout << "Success" << std::endl;
				break;
			case SQL_NO_DATA:
				std::cout << "no data" << std::endl;
				break;
			case SQL_INVALID_HANDLE:
				std::cout << "invalid" << std::endl;
				break;
			case SQL_STILL_EXECUTING:
				std::cout << "still exec" << std::endl;
			}

		// fetch the data from each row, and store in the o_table	
		while (SQLFetch(sqlstatementhandle) == SQL_SUCCESS)
		{
			
			double acc_data= Column2Numeric(sqlstatementhandle, 0, AGGREGATE_NAME);
			o_table[0].push_back(acc_data);
			//std::cout << "acc data" << acc_data;
			for (int ci = 1; ci < COL_NUM; ci++)
			{
				double data = Column2Numeric(sqlstatementhandle, ci, CONDITION_NAMES[ci-1]);
				o_table[ci].push_back(data);
			}
		}
		double read_time = ((double)clock() - st) / CLOCKS_PER_SEC;
		
		SQLFreeHandle(SQL_HANDLE_STMT, sqlstatementhandle);
		return read_time;
	}

	double SqlInterface::ReadBLBDb(SQLHANDLE& sqlconnectionhandle, std::vector<std::vector<double>>& o_table, std::string db_name, std::string table_name, std::string AGGREGATE_NAME, std::vector<std::string> CONDITION_NAMES)
	{
		SQLHANDLE sqlstatementhandle = NULL;
		if (SQLAllocHandle(SQL_HANDLE_STMT, sqlconnectionhandle, &sqlstatementhandle) != SQL_SUCCESS) return -1;

		double st = clock();
		o_table = std::vector<std::vector<double>>();
		std::string query = ReadTableStr(db_name, table_name, AGGREGATE_NAME, CONDITION_NAMES);
		SqlInterface::SqlQuery(query, sqlstatementhandle);
		short int COL_NUM = 0;
		SQLNumResultCols(sqlstatementhandle, &COL_NUM);
		std::cout << "colnum " << COL_NUM;
		for (int i = 0; i < COL_NUM; i++)
			o_table.push_back(std::vector<double>());
		std::cout << " otable[ci] " << o_table[0].size() << std::endl;
		switch (SQLFetch(sqlstatementhandle))
		{
		case SQL_SUCCESS:
			std::cout << "Success" << std::endl;
			break;
		case SQL_NO_DATA:
			std::cout << "no data" << std::endl;
			break;
		case SQL_INVALID_HANDLE:
			std::cout << "invalid" << std::endl;
			break;
		case SQL_STILL_EXECUTING:
			std::cout << "still exec" << std::endl;
		}



		while (SQLFetch(sqlstatementhandle) == SQL_SUCCESS)
		{
			double acc_data = Column2Numeric(sqlstatementhandle, 0, AGGREGATE_NAME);
			o_table[0].push_back(acc_data);
			//std::cout << "acc data" << acc_data;
			for (int ci = 1; ci < COL_NUM; ci++)
			{
				double data = Column2Numeric(sqlstatementhandle, ci, CONDITION_NAMES[ci - 1]);
				o_table[ci].push_back(data);
			}
		}
		double read_time = ((double)clock() - st) / CLOCKS_PER_SEC;

		SQLFreeHandle(SQL_HANDLE_STMT, sqlstatementhandle);
		return read_time;
	}
}