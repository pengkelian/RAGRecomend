from fastapi import FastAPI, HTTPException, Query
from typing import Optional
import pymysql.cursors

from MySQLDatabase import MySQLDatabase

app = FastAPI()
def connect_db():
    db = MySQLDatabase('43.138.111.201', 'root', 'PKL.19881001', 'llm_product')
    return db

from pydantic import BaseModel, Field
from typing import Optional

class Product(BaseModel):
    brand: Optional[str] = Field(None, min_length=1)
    price: Optional[float] = None
    title: Optional[str] = Field(None, min_length=1)
    rank: Optional[str] = Field(None, min_length=1)

@app.post("/products/search")
def read_products(product: Product):
    """
    根据品牌、价格、标题（模糊匹配）和排名搜索产品信息。
    至少需要一个参数不为空。
    """
    query_conditions = []
    params = []
    product_dict = product.dict()
    brand = product_dict.get('brand')
    price = product_dict.get('price')
    title = product_dict.get('title')
    rank = product_dict.get('rank')
    if brand:
        query_conditions.append("brand = '{}'".format(brand))
    if price is not None:
        query_conditions.append("price = {}".format(price))
    if title:
        query_conditions.append("title LIKE '%{}%'".format(title.replace("'", "''")))  # 转义单引号以防止字符串注入
    if rank:
        query_conditions.append("`rank` = {}".format(rank))

    if not query_conditions:
        raise HTTPException(status_code=400, detail="At least one query parameter must be provided")
    # 利用params参数，将参数插入到SQL语句中

    sql = f"SELECT * FROM products WHERE {' AND '.join(query_conditions)}"
    print("params:",params)


    print("sql:",sql)
    connection = connect_db()
    result = connection.fetch_data(sql)
    print("result:",result)
    return result
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=6020)
