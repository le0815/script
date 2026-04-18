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
    col_pattern = re.compile(r'\[(\w+)\]\s*\[(\w+)\](\s*\([0-9\s,]+\))?\s*(NOT\s+NULL|NULL)?', re.IGNORECASE)
    
    columns =[]
    for match in col_pattern.finditer(sql_core):
        col_name = match.group(1)
        col_type = match.group(2).lower()
        col_size = match.group(3).strip() if match.group(3) else ""
        col_constraints = match.group(4) if match.group(4) else ""
        
        doris_type = type_mapping.get(col_type, col_type)
        if col_type == 'uniqueidentifier':
            col_size = "" 
            
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
    priority_cols = ['DATAAREAID', 'RECID', 'RECVERSION']
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
    
    # THÊM 2 DÒNG THỐNG KÊ SỐ LƯỢNG CỘT TẠI ĐÂY
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
CREATE TABLE [dbo].[INVENTLOCATION](
	[ACTIVITYTYPE_RU] [nvarchar](30) NOT NULL,
	[ALLOWLABORSTANDARDS] [int] NOT NULL,
	[ALLOWMARKINGRESERVATIONREMOVAL] [int] NOT NULL,
	[BRANCHNUMBER] [nvarchar](13) NOT NULL,
	[CONSOLIDATESHIPATRTW] [int] NOT NULL,
	[CUSTACCOUNT_BR] [nvarchar](20) NOT NULL,
	[CUSTACCOUNT_HU] [nvarchar](20) NOT NULL,
	[CYCLECOUNTALLOWPALLETMOVE] [int] NOT NULL,
	[DECREMENTLOADLINE] [int] NOT NULL,
	[DEFAULTKANBANFINISHEDGOODSLOCATION] [nvarchar](20) NOT NULL,
	[DEFAULTPRODUCTIONFINISHGOODSLOCATION] [nvarchar](20) NOT NULL,
	[DEFAULTSHIPMAINTENANCELOC] [nvarchar](20) NOT NULL,
	[DEFAULTSTATUSID] [nvarchar](10) NOT NULL,
	[EMPTYPALLETLOCATION] [nvarchar](10) NOT NULL,
	[FSHSTORE] [int] NOT NULL,
	[INVENTCOUNTINGGROUP_BR] [int] NOT NULL,
	[INVENTLOCATIONID] [nvarchar](10) NOT NULL,
	[INVENTLOCATIONIDGOODSINROUTE_RU] [nvarchar](10) NOT NULL,
	[INVENTLOCATIONIDQUARANTINE] [nvarchar](10) NOT NULL,
	[INVENTLOCATIONIDREQMAIN] [nvarchar](10) NOT NULL,
	[INVENTLOCATIONIDTRANSIT] [nvarchar](10) NOT NULL,
	[INVENTLOCATIONLEVEL] [int] NOT NULL,
	[INVENTLOCATIONTYPE] [int] NOT NULL,
	[INVENTPROFILEID_RU] [nvarchar](10) NOT NULL,
	[INVENTPROFILETYPE_RU] [int] NOT NULL,
	[INVENTSITEID] [nvarchar](10) NOT NULL,
	[MANUAL] [int] NOT NULL,
	[MAXPICKINGROUTETIME] [int] NOT NULL,
	[MAXPICKINGROUTEVOLUME] [numeric](32, 6) NOT NULL,
	[NAME] [nvarchar](100) NOT NULL,
	[NUMBERSEQUENCEGROUP_RU] [nvarchar](10) NOT NULL,
	[PICKINGLINETIME] [int] NOT NULL,
	[PRINTBOLBEFORESHIPCONFIRM] [int] NOT NULL,
	[PRODRESERVEONLYWHSE] [int] NOT NULL,
	[RBODEFAULTINVENTPROFILEID_RU] [nvarchar](10) NOT NULL,
	[RBODEFAULTWMSLOCATIONID] [nvarchar](20) NOT NULL,
	[RBODEFAULTWMSPALLETID] [nvarchar](18) NOT NULL,
	[REMOVEINVENTBLOCKINGONSTATUSCHANGE] [int] NOT NULL,
	[REQCALENDARID] [nvarchar](10) NOT NULL,
	[REQREFILL] [int] NOT NULL,
	[RESERVEATLOADPOST] [int] NOT NULL,
	[RETAILINVENTNEGFINANCIAL] [int] NOT NULL,
	[RETAILINVENTNEGPHYSICAL] [int] NOT NULL,
	[RETAILWEIGHTEX1] [numeric](32, 6) NOT NULL,
	[RETAILWMSLOCATIONIDDEFAULTRETURN] [nvarchar](20) NOT NULL,
	[RETAILWMSPALLETIDDEFAULTRETURN] [nvarchar](18) NOT NULL,
	[UNIQUECHECKDIGITS] [int] NOT NULL,
	[USEWMSORDERS] [int] NOT NULL,
	[VENDACCOUNT] [nvarchar](20) NOT NULL,
	[VENDACCOUNTCUSTOM_RU] [nvarchar](20) NOT NULL,
	[WHSENABLED] [int] NOT NULL,
	[WHSRAWMATERIALPOLICY] [int] NOT NULL,
	[WMSAISLENAMEACTIVE] [int] NOT NULL,
	[WMSLEVELFORMAT] [nvarchar](10) NOT NULL,
	[WMSLEVELNAMEACTIVE] [int] NOT NULL,
	[WMSLOCATIONIDDEFAULTISSUE] [nvarchar](20) NOT NULL,
	[WMSLOCATIONIDDEFAULTRECEIPT] [nvarchar](20) NOT NULL,
	[WMSLOCATIONIDGOODSINROUTE_RU] [nvarchar](20) NOT NULL,
	[WMSPOSITIONFORMAT] [nvarchar](10) NOT NULL,
	[WMSPOSITIONNAMEACTIVE] [int] NOT NULL,
	[WMSRACKFORMAT] [nvarchar](10) NOT NULL,
	[WMSRACKNAMEACTIVE] [int] NOT NULL,
	[WAREHOUSEAUTORELEASERESERVATION] [int] NOT NULL,
	[DEFAULTPRODUCTIONINPUTLOCATION] [nvarchar](20) NOT NULL,
	[DEFAULTRETURNCREDITONLYLOCATION] [nvarchar](20) NOT NULL,
	[DEFAULTCONTAINERTYPECODE] [nvarchar](20) NOT NULL,
	[RELEASETOWAREHOUSERULE] [int] NOT NULL,
	[WORKPROCESSINGPOLICYNAME] [nvarchar](60) NOT NULL,
	[AUTOUPDATESHIPMENT] [int] NOT NULL,
	[DEFAULTQUALITYMAINTENANCELOCATION] [nvarchar](20) NOT NULL,
	[ENABLEQUALITYMANAGEMENT] [int] NOT NULL,
	[LOADRELEASERESERVATIONPOLICY] [int] NOT NULL,
	[REJECTORDERFULFILLMENT] [nvarchar](10) NOT NULL,
	[ITMINVENTLOCATIONIDUNDER] [nvarchar](10) NOT NULL,
	[ITMINVENTLOCATIONIDGIT] [nvarchar](10) NOT NULL,
	[DATAAREAID] [nvarchar](4) NOT NULL,
	[PARTITION] [bigint] NOT NULL,
	[RECID] [bigint] NOT NULL,
	[RECVERSION] [int] NOT NULL,
	[MODIFIEDDATETIME] [datetime] NOT NULL,
	[MODIFIEDBY] [nvarchar](20) NOT NULL,
	[CREATEDDATETIME] [datetime] NOT NULL,
	[CREATEDBY] [nvarchar](20) NOT NULL,
	[GFT_WAREHOUSETYPE] [int] NOT NULL,
	[GDS_ISCALCSTOCK] [int] NOT NULL,
	[GDS_NOTJOINMOKITTINGCALC] [int] NOT NULL,
	[GFT_APPRVALDEPTID] [nvarchar](50) NOT NULL,
	[GDS_ABANDONEDWAREHOUSE] [int] NOT NULL,
	[GFT_ISCKPOSTNUMSAME] [int] NOT NULL,
	[GFT_INVENTLOCATION_TYPE] [int] NOT NULL,
 CONSTRAINT [I_23753INVENTLOCATIONIDX] PRIMARY KEY CLUSTERED 
(
	[PARTITION] ASC,
	[DATAAREAID] ASC,
	[INVENTLOCATIONID] ASC
)
"""

print(convert_sqlserver_to_doris(sql_server_script))