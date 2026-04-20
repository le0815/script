import re

def convert_sqlserver_to_doris(sql_text, table_prefix="D365_"):
    # 1. Tách phần khai báo cột (Cắt bỏ phần CONSTRAINT và PRIMARY KEY)
    sql_core = re.split(r'\bCONSTRAINT\b|\bPRIMARY\s+KEY\b', sql_text, maxsplit=1, flags=re.IGNORECASE)[0]
    
    # 2. Lấy tên bảng
    table_match = re.search(r'CREATE\s+TABLE\s*(?:\[?\w+\]?\.)?\s*\[?(\w+)\]?', sql_core, re.IGNORECASE)
    if not table_match:
        return "Lỗi: Không tìm thấy tên bảng trong câu lệnh SQL."
    
    table_name = table_prefix + table_match.group(1)
    
    # 3. Định nghĩa mapping các kiểu dữ liệu
    type_mapping = {
        'nvarchar': 'varchar',
        'nchar': 'varchar',
        'char': 'varchar',
        'varchar': 'varchar',
        'uniqueidentifier': 'varchar(36)',
        'int': 'int',
        'bigint': 'bigint',
        'smallint': 'smallint',
        'tinyint': 'tinyint',
        'bit': 'tinyint', 
        'datetime': 'datetime',
        'datetime2': 'datetime',
        'date': 'date',
        'decimal': 'decimal',
        'numeric': 'decimal',
        'float': 'double'
    }

    # 4. Trích xuất TẤT CẢ các cột
    col_pattern = re.compile(r'\[(\w+)\]\s*\[(\w+)\](\s*\([0-9\s,a-zA-Z]+\))?\s*(NOT\s+NULL|NULL)?', re.IGNORECASE)
    
    columns =[]
    for match in col_pattern.finditer(sql_core):
        col_name = match.group(1)
        col_type = match.group(2).lower()
        col_size = match.group(3).strip() if match.group(3) else ""
        col_constraints = match.group(4) if match.group(4) else ""
        
        doris_type = type_mapping.get(col_type, col_type)
        
        # ---------- LOGIC XỬ LÝ SIZE ĐẶC BIỆT ----------
        if col_type == 'uniqueidentifier':
            col_size = "" 
        elif col_type == 'nvarchar' and col_size:
            # Tìm con số trong chuỗi kích thước (VD: "(20)" -> lấy 20)
            num_match = re.search(r'\d+', col_size)
            if num_match:
                new_len = int(num_match.group()) * 3
                col_size = f"({new_len})"
            elif 'max' in col_size.lower():
                # Xử lý trường hợp nvarchar(max)
                doris_type = 'string'
                col_size = ""
        # -----------------------------------------------
            
        full_type = f"{doris_type}{col_size}"
        is_not_null = "NOT NULL" if "NOT NULL" in col_constraints.upper() else "NULL"
        
        columns.append({
            'name': col_name,
            'type': full_type,
            'constraint': is_not_null
        })

    if not columns:
        return "Lỗi: Không tìm thấy khai báo cột nào."

    # 5. Sắp xếp lại thứ tự cột cho Apache Doris
    priority_cols =['DATAAREAID', 'RECID', 'RECVERSION']
    sorted_columns =[]
    
    for pc in priority_cols:
        for col in columns:
            if col['name'].upper() == pc:
                sorted_columns.append(col)
                
    for col in columns:
        if col['name'].upper() not in priority_cols:
            sorted_columns.append(col)

    # 6. Build chuỗi câu lệnh cho Apache Doris
    doris_sql =[]
    
    doris_sql.append(f"-- sql totals column: {len(columns)}")
    doris_sql.append(f"-- doris totals column: {len(sorted_columns)}")
    
    # Build câu lệnh Create table
    doris_sql.append(f"CREATE TABLE IF NOT EXISTS {table_name} (")
    
    col_strings =[]
    for col in sorted_columns:
        col_strings.append(f"`{col['name']}` {col['type']} {col['constraint']}")
        
    doris_sql.append(",\n".join(col_strings))
    doris_sql.append(")")
    doris_sql.append("ENGINE=OLAP")
    doris_sql.append("UNIQUE KEY(`DATAAREAID`, `RECID`)")
    doris_sql.append("DISTRIBUTED BY HASH(`DATAAREAID`) BUCKETS 1")
    doris_sql.append("PROPERTIES (")
    doris_sql.append('"replication_allocation" = "tag.location.default: 1"')
    doris_sql.append(");")

    return "\n".join(doris_sql)

# ==========================================
# TEST VỚI DỮ LIỆU ĐẦU VÀO CỦA BẠN
# ==========================================
sql_server_script = """
CREATE TABLE[dbo].[DLVREASON](
	[CODE][nvarchar](10) NOT NULL,[TXT] [nvarchar](200) NOT NULL,[FREE_IT][int] NOT NULL,
	[INVOICEACCOUNT_IT] [nvarchar](20) NOT NULL,[PAYMTERMID_IT] [nvarchar](100) NOT NULL,[DATAAREAID][nvarchar](4) NOT NULL,
	[PARTITION] [bigint] NOT NULL,[RECID] [bigint] NOT NULL,
	[RECVERSION] [int] NOT NULL,[GDS_INVENTLOCATIONID] [nvarchar](10) NOT NULL,
 CONSTRAINT[I_1494CODEIDX] PRIMARY KEY CLUSTERED 
(
	[PARTITION] ASC,[DATAAREAID] ASC,
)
"""

print(convert_sqlserver_to_doris(sql_server_script))