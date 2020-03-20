from data_lineage.parser.parser import parse
from data_lineage.parser.visitor import Visitor

sql = """
SELECT ca_zip, 
               Sum(cs_sales_price) 
FROM   catalog_sales, 
       customer, 
       customer_address, 
       date_dim 
WHERE  cs_bill_customer_sk = c_customer_sk 
       AND c_current_addr_sk = ca_address_sk 
       AND ( Substr(ca_zip, 1, 5) IN ( '85669', '86197', '88274', '83405', 
                                       '86475', '85392', '85460', '80348', 
                                       '81792' ) 
              OR ca_state IN ( 'CA', 'WA', 'GA' ) 
              OR cs_sales_price > 500 ) 
       AND cs_sold_date_sk = d_date_sk 
       AND d_qoy = 1 
       AND d_year = 1998 
GROUP  BY ca_zip 
ORDER  BY ca_zip
LIMIT 100; 
"""


class PrintingVisitor(Visitor):
    @classmethod
    def visit_list(cls, obj):
        print(obj)

    @classmethod
    def visit_node(cls, obj):
        print(obj)

    @classmethod
    def visit_scalar(cls, obj):
        print(obj)


def test_parser():
    node = parse(sql)
    node.accept(PrintingVisitor)

    assert False
