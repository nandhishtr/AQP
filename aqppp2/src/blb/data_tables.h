//
// Created by hti on 10/4/23.
//
#pragma once
#include <iostream>
#include <cstdlib>
#include<cstring>
#include <chrono>
#include <random>
#include <fstream>
#include <string>
#include <vector>
#include <cmath>
#include <omp.h>
#include <string>
#include <map>

struct sample_struct_lineitem {
    std::vector<int>            l_orderkey;
    std::vector<int>            l_partkey;
    std::vector<int>            l_suppkey;
    std::vector<int>            l_linenumber;
    std::vector<float>          l_quantity;
    std::vector<float>          l_extendedprice;
    std::vector<float>          l_discount;
    std::vector<float>          l_tax;
    std::vector<char>           l_returnflag;
    std::vector<char>           l_linestatus;
    std::vector<int>            l_shipdate;
    std::vector<int>            l_commidate;
    std::vector<int>            l_receiptdate;
    std::vector<std::string>    l_shipinstruct;
    std::vector<std::string>    l_shipmode;
    std::vector<std::string>    l_comment;
    std::vector<int>            id;
    std::vector<float>          coeff;
    std::vector<float>          answer;
    size_t                      sample_size;

    explicit sample_struct_lineitem(std::size_t size);
    sample_struct_lineitem(const sample_struct_lineitem& other);
    void clearData();
    ~sample_struct_lineitem();
};


//------Part
struct sample_struct_part {
    std::vector<int>            p_partkey;
    std::vector<std::string>    p_name;
    std::vector<std::string>    p_mfgr;
    std::vector<std::string>    p_brand;
    std::vector<std::string>    p_type;
    std::vector<int>            p_size;
    std::vector<std::string>    p_container;
    std::vector<float>          p_retailprice;
    std::vector<std::string>    p_comment;
    std::vector<int>            id;
    std::vector<float>          coeff;
    std::vector<float>          answer;
    size_t                      sample_size;

    explicit sample_struct_part(std::size_t size);
    sample_struct_part(const sample_struct_part& other);
    void clearData();
    ~sample_struct_part();
};


//------Supplier
struct sample_struct_supplier {
    std::vector<int>            s_suppkey;
    std::vector<std::string>    s_name;
    std::vector<std::string>    s_address;
    std::vector<int>            s_nationkey;
    std::vector<std::string>    s_phone;
    std::vector<int>            s_acctbal;
    std::vector<std::string>    s_comment;
    std::vector<int>            id;
    std::vector<float>          coeff;
    std::vector<float>          answer;
    size_t                      sample_size;

    explicit sample_struct_supplier(std::size_t size);
    sample_struct_supplier(const sample_struct_supplier& other);
    void clearData();
    ~sample_struct_supplier();
};

//------Partsupp
struct sample_struct_partsupp {
    std::vector<int>            ps_partkey;
    std::vector<int>            ps_suppkey;
    std::vector<int>            ps_availqty;
    std::vector<float>          ps_supplycost;
    std::vector<std::string>    ps_comment;
    std::vector<int>            id;
    std::vector<float>          coeff;
    std::vector<float>          answer;
    size_t                      sample_size;

    explicit sample_struct_partsupp(std::size_t size);
    sample_struct_partsupp(const sample_struct_partsupp& other);
    void clearData();
    ~sample_struct_partsupp();
};

//------Customer
struct sample_struct_customer{
    std::vector<int>            c_custkey;
    std::vector<std::string>    c_name;
    std::vector<std::string>    c_address;
    std::vector<int>            c_nationkey;
    std::vector<std::string>    c_phone;
    std::vector<float>          c_acctbal;
    std::vector<std::string>    c_mktsegment;
    std::vector<std::string>    c_comment;
    std::vector<int>            id;
    std::vector<float>          coeff;
    std::vector<float>          answer;
    size_t                      sample_size;

    explicit sample_struct_customer(std::size_t size);
    sample_struct_customer(const sample_struct_customer& other);
    void clearData();
    ~sample_struct_customer();
};

//------Orders
struct sample_struct_orders{
    std::vector<int>            o_orderkey;
    std::vector<int>            o_custkey;
    std::vector<char>           o_orderstatus;
    std::vector<float>          o_totalprice;
    std::vector<int>            o_orderdate;
    std::vector<std::string>    o_orderpriority;
    std::vector<std::string>    o_clerk;
    std::vector<int>            o_shippriority;
    std::vector<std::string>    o_comment;

    std::vector<int>            id;
    std::vector<float>          coeff;
    std::vector<float>          answer;
    size_t                      sample_size;

    explicit sample_struct_orders(std::size_t size);
    sample_struct_orders(const sample_struct_orders& other);
    void clearData();
    ~sample_struct_orders();
};

//------Nation
struct sample_struct_nation{
    std::vector<int>            n_nationkey;
    std::vector<std::string>    n_name;
    std::vector<int>            n_regionkey;
    std::vector<std::string>    n_comment;

    std::vector<int>            id;
    std::vector<float>          coeff;
    std::vector<float>          answer;
    size_t                      sample_size;

    explicit sample_struct_nation(std::size_t size);
    sample_struct_nation(const sample_struct_nation& other);
    void clearData();
    ~sample_struct_nation();
};

//------Region
struct sample_struct_region{
    std::vector<int>            r_regionkey;
    std::vector<std::string>    r_name;
    std::vector<std::string>    r_comment;

    std::vector<int>            id;
    std::vector<float>          coeff;
    std::vector<float>          answer;
    size_t                      sample_size;

    explicit sample_struct_region(std::size_t size);
    sample_struct_region(const sample_struct_region& other);
    void clearData();
    ~sample_struct_region();
};

//------
struct tpc_h_data_struct{

    sample_struct_customer  customer_table;
    sample_struct_lineitem  lineitem_table;
    sample_struct_nation    nation_table;
    sample_struct_orders    orders_table;
    sample_struct_part      part_table;
    sample_struct_partsupp  partsupp_table;
    sample_struct_region    region_table;
    sample_struct_supplier  supplier_table;
    //size_t                  sample_size{};

    explicit tpc_h_data_struct(std::size_t size) :
            customer_table(size),
            lineitem_table(size),
            nation_table(size),
            orders_table(size),
            part_table(size),
            partsupp_table(size),
            region_table(size),
            supplier_table(size) {}

    //explicit tpc_h_data_struct(std::size_t size);
    ~tpc_h_data_struct();
    void clearData();
};


