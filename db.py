import pymysql,json,csv

def get_conn():
    # 尝试打开并从db_config.json读取数据库配置
    try:
        with open('db_config.json', 'r') as f:
            config = json.load(f)
        try:
            connection = pymysql.connect(**config)
            return connection
        except Exception as e:
            raise Exception("Error connecting to the database:", str(e)) from None
    except (IOError, json.JSONDecodeError) as e:
        raise Exception("无法读取数据库配置文件: " + str(e)) from None

# def get_table_row_count(table_name):
#     conn = get_conn()
#     try:
#         cursor = conn.cursor(pymysql.cursors.DictCursor)
#         cursor.execute(f"SELECT COUNT(*) AS `row_count` FROM {table_name}")
#         row_count = cursor.fetchone()['row_count']
#         return row_count
#     finally:
#         if conn:
#             conn.close()

def save_table_to_csv(table_name, csv_file_name):
    conn = get_conn()
    try:
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        # 构建查询所有行的SQL语句
        query = f"SELECT * FROM `{table_name}`"
        
        cursor.execute(query)
        # 获取查询结果的所有行
        rows = cursor.fetchall()
        
        # 打开CSV文件，准备写入
        with open(csv_file_name, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            # 写入列名
            writer.writerow([row.keys() for row in rows][0])
            # 写入所有行数据
            for row in rows:
                writer.writerow(row.values())
    except Exception as e:
        print(f"Error saving table to CSV: {e}")
    finally:
        # 关闭数据库连接
        if conn:
            conn.close()

def query_data(sql):
    # 获取数据库连接
    conn = get_conn()
    try:
        # 创建一个游标对象，以字典形式执行SQL查询和获取结果
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        # 执行SQL查询
        cursor.execute(sql)
        
        # 获取查询结果的所有行
        return cursor.fetchall()
    finally:
        # 关闭数据库连接
        conn.close()

def insert_or_update(sql):
    # 获取数据库连接
    conn = get_conn()
    try:
        # 创建一个游标对象，执行SQL插入或更新操作
        cursor = conn.cursor()
        
        # 执行SQL插入或更新操作
        cursor.execute(sql)
        
        # 提交更改到数据库
        conn.commit()
    finally:
        # 关闭数据库连接
        conn.close()