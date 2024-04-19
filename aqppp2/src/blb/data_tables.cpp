//
// Created by hti on 10/4/23.
//
#include "data_tables.h"


//------Lineitem
sample_struct_lineitem::sample_struct_lineitem(std::size_t size) {

    l_orderkey.reserve(size);
    l_partkey.reserve(size);
    l_suppkey.reserve(size);
    l_linenumber.reserve(size);
    l_quantity.reserve(size);
    l_extendedprice.reserve(size);
    l_discount.reserve(size);
    l_tax.reserve(size);
    l_returnflag.reserve(size);
    l_linestatus.reserve(size);
    l_shipdate.reserve(size);
    l_commidate.reserve(size);
    l_receiptdate.reserve(size);
    l_shipinstruct.reserve(size);
    l_shipmode.reserve(size);
    l_comment.reserve(size);
    coeff.reserve(size);
    answer.reserve(size);
    id.reserve(size);
    sample_size = size;
}

sample_struct_lineitem::sample_struct_lineitem(const sample_struct_lineitem& other) {

    l_orderkey = other.l_orderkey;
    l_partkey = other.l_partkey;
    l_suppkey = other.l_suppkey;
    l_linenumber = other.l_linenumber;
    l_quantity = other.l_quantity;
    l_extendedprice = other.l_extendedprice;
    l_discount = other.l_discount;
    l_tax = other.l_tax;
    l_returnflag = other.l_returnflag;
    l_linestatus = other.l_linestatus;
    l_shipdate = other.l_shipdate;
    l_commidate = other.l_commidate;
    l_receiptdate = other.l_receiptdate;
    l_shipinstruct = other.l_shipinstruct;
    l_shipmode = other.l_shipmode;
    l_comment = other.l_comment;
    id = other.id;
    coeff = other.coeff;
    answer = other.answer;
    sample_size = other.sample_size;
}

sample_struct_lineitem::~sample_struct_lineitem() {

    l_orderkey.clear();
    l_partkey.clear();
    l_suppkey.clear();
    l_linenumber.clear();
    l_quantity.clear();
    l_extendedprice.clear();
    l_discount.clear();
    l_tax.clear();
    l_returnflag.clear();
    l_linestatus.clear();
    l_shipdate.clear();
    l_commidate.clear();
    l_receiptdate.clear();
    l_shipinstruct.clear();
    l_shipmode.clear();
    l_comment.clear();
    id.clear();
    coeff.clear();
    answer.clear();
}

void sample_struct_lineitem::clearData() {

    l_orderkey.clear();
    l_partkey.clear();
    l_suppkey.clear();
    l_linenumber.clear();
    l_quantity.clear();
    l_extendedprice.clear();
    l_discount.clear();
    l_tax.clear();
    l_returnflag.clear();
    l_linestatus.clear();
    l_shipdate.clear();
    l_commidate.clear();
    l_receiptdate.clear();
    l_shipinstruct.clear();
    l_shipmode.clear();
    l_comment.clear();
    id.clear();
    coeff.clear();
    answer.clear();
}



//------Part

sample_struct_part::sample_struct_part(std::size_t size) {

    p_partkey.reserve(size);
    p_name.reserve(size);
    p_mfgr.reserve(size);
    p_brand.reserve(size);
    p_type.reserve(size);
    p_size.reserve(size);
    p_container.reserve(size);
    p_retailprice.reserve(size);
    p_comment.reserve(size);
    coeff.reserve(size);
    answer.reserve(size);
    id.reserve(size);
    sample_size = size;
}

sample_struct_part::sample_struct_part(const sample_struct_part& other) {

    p_partkey = other.p_partkey;
    p_name = other.p_name;
    p_mfgr = other.p_mfgr;
    p_brand = other.p_brand;
    p_type = other.p_type;
    p_size = other.p_size;
    p_container = other.p_container;
    p_retailprice = other.p_retailprice;
    p_comment = other.p_comment;
    id = other.id;
    coeff = other.coeff;
    answer = other.answer;
    sample_size = other.sample_size;
}

sample_struct_part::~sample_struct_part() {

    p_partkey.clear();
    p_name.clear();
    p_mfgr.clear();
    p_brand.clear();
    p_type.clear();
    p_size.clear();
    p_container.clear();
    p_retailprice.clear();
    p_comment.clear();
    id.clear();
    coeff.clear();
    answer.clear();
}

void sample_struct_part::clearData() {

    p_partkey.clear();
    p_name.clear();
    p_mfgr.clear();
    p_brand.clear();
    p_type.clear();
    p_size.clear();
    p_container.clear();
    p_retailprice.clear();
    p_comment.clear();
    id.clear();
    coeff.clear();
    answer.clear();
}

//------Supplier

sample_struct_supplier::sample_struct_supplier(std::size_t size) {

    s_suppkey.reserve(size);
    s_name.reserve(size);
    s_address.reserve(size);
    s_nationkey.reserve(size);
    s_phone.reserve(size);
    s_acctbal.reserve(size);
    s_comment.reserve(size);
    coeff.reserve(size);
    answer.reserve(size);
    id.reserve(size);
    sample_size = size;
}

sample_struct_supplier::sample_struct_supplier(const sample_struct_supplier& other) {

    s_suppkey = other.s_suppkey;
    s_name = other.s_name;
    s_address = other.s_address;
    s_nationkey = other.s_nationkey;
    s_phone = other.s_phone;
    s_acctbal = other.s_acctbal;
    s_comment = other.s_comment;
    id = other.id;
    coeff = other.coeff;
    answer = other.answer;
    sample_size = other.sample_size;
}

sample_struct_supplier::~sample_struct_supplier() {

    s_suppkey.clear();
    s_name.clear();
    s_address.clear();
    s_nationkey.clear();
    s_phone.clear();
    s_acctbal.clear();
    s_comment.clear();
    id.clear();
    coeff.clear();
    answer.clear();
}

void sample_struct_supplier::clearData() {

    s_suppkey.clear();
    s_name.clear();
    s_address.clear();
    s_nationkey.clear();
    s_phone.clear();
    s_acctbal.clear();
    s_comment.clear();
    id.clear();
    coeff.clear();
    answer.clear();
}

//------Partsupp
sample_struct_partsupp::sample_struct_partsupp(std::size_t size) {

    ps_partkey.reserve(size);
    ps_suppkey.reserve(size);
    ps_availqty.reserve(size);
    ps_supplycost.reserve(size);
    ps_comment.reserve(size);
    coeff.reserve(size);
    answer.reserve(size);
    id.reserve(size);
    sample_size = size;
}

sample_struct_partsupp::sample_struct_partsupp(const sample_struct_partsupp& other) {

    ps_partkey = other.ps_partkey;
    ps_suppkey = other.ps_suppkey;
    ps_availqty = other.ps_availqty;
    ps_supplycost = other.ps_supplycost;
    ps_comment = other.ps_comment;
    id = other.id;
    coeff = other.coeff;
    answer = other.answer;
    sample_size = other.sample_size;
}

sample_struct_partsupp::~sample_struct_partsupp() {

    ps_partkey.clear();
    ps_suppkey.clear();
    ps_availqty.clear();
    ps_supplycost.clear();
    ps_comment.clear();
    id.clear();
    coeff.clear();
    answer.clear();
}

void sample_struct_partsupp::clearData() {

    ps_partkey.clear();
    ps_suppkey.clear();
    ps_availqty.clear();
    ps_supplycost.clear();
    ps_comment.clear();
    id.clear();
    coeff.clear();
    answer.clear();
}

//------Customer
sample_struct_customer::sample_struct_customer(std::size_t size) {

    c_custkey.reserve(size);
    c_name.reserve(size);
    c_address.reserve(size);
    c_nationkey.reserve(size);
    c_phone.reserve(size);
    c_acctbal.reserve(size);
    c_mktsegment.reserve(size);
    c_comment.reserve(size);
    coeff.reserve(size);
    answer.reserve(size);
    id.reserve(size);
    sample_size = size;
}

sample_struct_customer::sample_struct_customer(const sample_struct_customer& other) {

    c_custkey = other.c_custkey;
    c_name = other.c_name;
    c_address = other.c_address;
    c_nationkey = other.c_nationkey;
    c_phone = other.c_phone;
    c_acctbal = other.c_acctbal;
    c_mktsegment = other.c_mktsegment;
    c_comment = other.c_comment;
    id = other.id;
    coeff = other.coeff;
    answer = other.answer;
    sample_size = other.sample_size;
}

sample_struct_customer::~sample_struct_customer() {

    c_custkey.clear();
    c_name.clear();
    c_address.clear();
    c_nationkey.clear();
    c_phone.clear();
    c_acctbal.clear();
    c_mktsegment.clear();
    c_comment.clear();
    id.clear();
    coeff.clear();
    answer.clear();
}

void sample_struct_customer::clearData() {

    c_custkey.clear();
    c_name.clear();
    c_address.clear();
    c_nationkey.clear();
    c_phone.clear();
    c_acctbal.clear();
    c_mktsegment.clear();
    c_comment.clear();
    id.clear();
    coeff.clear();
    answer.clear();
}

//------Orders
sample_struct_orders::sample_struct_orders(std::size_t size) {

    o_orderkey.reserve(size);
    o_custkey.reserve(size);
    o_orderstatus.reserve(size);
    o_totalprice.reserve(size);
    o_orderdate.reserve(size);
    o_orderpriority.reserve(size);
    o_clerk.reserve(size);
    o_shippriority.reserve(size);
    o_comment.reserve(size);
    coeff.reserve(size);
    answer.reserve(size);
    id.reserve(size);
    sample_size = size;
}

sample_struct_orders::sample_struct_orders(const sample_struct_orders& other) {

    o_orderkey = other.o_orderkey;
    o_custkey = other.o_custkey;
    o_orderstatus = other.o_orderstatus;
    o_totalprice = other.o_totalprice;
    o_orderdate = other.o_orderdate;
    o_orderpriority = other.o_orderpriority;
    o_clerk = other.o_clerk;
    o_shippriority = other.o_shippriority;
    o_comment = other.o_comment;
    id = other.id;
    coeff = other.coeff;
    answer = other.answer;
    sample_size = other.sample_size;
}

sample_struct_orders::~sample_struct_orders() {

    o_orderkey.clear();
    o_custkey.clear();
    o_orderstatus.clear();
    o_totalprice.clear();
    o_orderdate.clear();
    o_orderpriority.clear();
    o_clerk.clear();
    o_shippriority.clear();
    o_comment.clear();

    id.clear();
    coeff.clear();
    answer.clear();
}

void sample_struct_orders::clearData() {

    o_orderkey.clear();
    o_custkey.clear();
    o_orderstatus.clear();
    o_totalprice.clear();
    o_orderdate.clear();
    o_orderpriority.clear();
    o_clerk.clear();
    o_shippriority.clear();
    o_comment.clear();

    id.clear();
    coeff.clear();
    answer.clear();
}

//------Nation
sample_struct_nation::sample_struct_nation(std::size_t size) {

    n_nationkey.reserve(size);
    n_name.reserve(size);
    n_regionkey.reserve(size);
    n_comment.reserve(size);

    coeff.reserve(size);
    answer.reserve(size);
    id.reserve(size);
    sample_size = size;
}

sample_struct_nation::sample_struct_nation(const sample_struct_nation& other) {

    n_nationkey = other.n_nationkey;
    n_name = other.n_name;
    n_regionkey = other.n_regionkey;
    n_comment = other.n_comment;

    id = other.id;
    coeff = other.coeff;
    answer = other.answer;
    sample_size = other.sample_size;
}

sample_struct_nation::~sample_struct_nation() {

    n_nationkey.clear();
    n_name.clear();
    n_regionkey.clear();
    n_comment.clear();

    id.clear();
    coeff.clear();
    answer.clear();
}

void sample_struct_nation::clearData() {

    n_nationkey.clear();
    n_name.clear();
    n_regionkey.clear();
    n_comment.clear();

    id.clear();
    coeff.clear();
    answer.clear();
}

//------Region
sample_struct_region::sample_struct_region(std::size_t size) {

    r_regionkey.reserve(size);
    r_name.reserve(size);
    r_comment.reserve(size);

    coeff.reserve(size);
    answer.reserve(size);
    id.reserve(size);
    sample_size = size;
}

sample_struct_region::sample_struct_region(const sample_struct_region& other) {

    r_regionkey = other.r_regionkey;
    r_name = other.r_name;
    r_comment = other.r_comment;

    id = other.id;
    coeff = other.coeff;
    answer = other.answer;
    sample_size = other.sample_size;
}

sample_struct_region::~sample_struct_region() {

    r_regionkey.clear();
    r_name.clear();
    r_comment.clear();

    id.clear();
    coeff.clear();
    answer.clear();
}

void sample_struct_region::clearData() {

    r_regionkey.clear();
    r_name.clear();
    r_comment.clear();

    id.clear();
    coeff.clear();
    answer.clear();
}

//------TPC-H data

tpc_h_data_struct::~tpc_h_data_struct() = default;
void tpc_h_data_struct::clearData() {
    customer_table.clearData();
    lineitem_table.clearData();
    nation_table.clearData();
    orders_table.clearData();
    part_table.clearData();
    partsupp_table.clearData();
    region_table.clearData();
    supplier_table.clearData();
}
