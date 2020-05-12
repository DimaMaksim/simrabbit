import pyodbc
import pandas as pd


def connect_DB(database,uid,pwd,driver,server):
    try:
        conn=pyodbc.connect(driver=driver,server=server,database=database,uid=uid,pwd=pwd)
    except Exception as er:
        print("In connection",  er)
    return conn

def get_sales(articule, filial, start_date, end_date,conn):
    sql_query_sales_by_articule_and_filial = f"select [Date] as Date, \
            QtySales as quantity, \
            QtyReturns as returns, \
            AmountSales,AmountReturns,  \
            StoreQtyDefault,\
            PriceOut,\
            AmountDefault as StoreAmountDefault, \
            case when ActivityId<>0 then 1 else 0 end as promo_days\
            from [SalesHub.Dev].[DataHub].[v_SalesStores] \
            where LagerId={articule}\
            and FilialId ={filial}\
            and [Date]>={start_date} and [Date]<={end_date}\
            order by [Date]"
    df = pd.read_sql_query(sql_query_sales_by_articule_and_filial, conn)
    df_val=df.values
    df_col=df.columns
    return (df_val,df_col)



def get_filter_sales(articule, filial, start_date, end_date,conn):
    query = f"select OperationDate as Date, \
            FilteredValues as filt_quantity FROM [SalesHub.Dev].[DataHub].[FilteredSalesTestForSimulation] \
            where LagerId={articule}\
            and FilialId ={filial}\
            and OperationDate>={start_date} and OperationDate<={end_date}"
    df = pd.read_sql_query(query, conn)
    return df

def get_init_store(articule, filial, date,conn):
    sql_query_init_store = f"select StoreQtyDefault \
            from [SalesHub.Dev].[DataHub].[v_SalesStores] \
            where LagerId={articule} and FilialId ={filial} and [Date]={date}"
    df = pd.read_sql_query(sql_query_init_store, conn)
    return df


def get_aoengine_name_(filid,conn):
    sql=f"exec [AO].[GetForecastTypes] ?"
    df=pd.read_sql_query(sql,params=(filid,),con=conn)
    return df
def get_aoengine_name(lagerid,filid,conn):
    sql=f"""SET NOCOUNT ON declare @tabl table(lagerid int, clasid int,filtype int,engintype char(64), forecasttype int)
        insert @tabl exec [AO].[GetForecastTypes] {filid}
        select * from @tabl where lagerid={lagerid} """
    df=pd.read_sql(sql,con=conn)
    return df

def deliv_shedule(articule,filid,start_date,end_date,conn):
    sql="""select * from MD.dbo.delivdates(?,?,?,?)"""
    params=(articule,filid,start_date,end_date)
    df=pd.read_sql_query(sql,params=params,con=conn)
    return df
def av_period(articule,filid,conn):
    sql="select * from MD.dbo.avperiod(?,?)"
    params=(articule,filid)
    df=pd.read_sql_query(sql,params=params,con=conn)
    if len(df)!=0:
        df=df.loc[df.modified==df.modified.max()]
        return df.L_T.values[0],df.delta.values[0]
    else:
        return [1,1]



def get_promo_days(articule, filial, start_date, end_date,conn):
    query = f"select [date_id], \
            promotion_type \
            from [4t.Dev].[4t_data].[promos] \
            where product_id = {articule}\
            and store_id = {filial}\
            and [date_id] >= {start_date} and [date_id] <= {end_date}"
    df = pd.read_sql_query(query, con=conn)
    return df

def get_rasf(articule, filial,conn):
    query = f"SELECT B.rasf FROM [4t.Dev].[4t].[rasf] as A\
            join [4t.Dev].[4t].[rasfid] as B on A.rasfid=B.rasfid\
            where A.filid={filial} and A.lagerid= {articule}"
    df = pd.read_sql_query(query, con=conn)
    return df

def Get_ForecastSalesForSimulation(filid,articule,start_date,end_date,conn):
    sql=f"exec [AO].[GetForecastSalesForSimulation] ?, ?, ?, ?"
    df=pd.read_sql_query(sql,params=(filid,articule,start_date,end_date,),con=conn)
    return df


def Salesmean(articule,filid,conn):
    sql=f"exec [dbo].[Salesmean] ?, ?"
    df=pd.read_sql_query(sql,params=(articule,filid),con=conn)
    return df
def getGodnost(articule,conn):
    sql=f"SELECT[UsedByDay] FROM [InventorySimul].[dbo].[GodnostDays] where lagerid={articule}"
    df=pd.read_sql_query(sql,con=conn)
    return df



def getLagerName(articule,conn):
    sql=f"select [lagerName],[lagerClassifier] \
            from [MasterData].[sku].[Lagers] \
            where lagerid = {articule}"
    df = pd.read_sql_query(sql, con=conn)
    return df

def get_ZP(articule, filial,conn):
    query = f"SELECT ZP,SSL FROM [InventorySimul].[dbo].[ZP_SSL]\
            where Filid={filial} and Lagerid= {articule}"
    df = pd.read_sql_query(query, con=conn)
    return df

def get_Action_(articule, filial,conn):
    query = f"SELECT [PriceListDate],[PriceListEndDate],[coef] FROM [InventorySimul].[dbo].[MarketProgramCoef]\
            where [FilID]={filial} and [LagerID]={articule} and [coef]<>1\
            order by [PriceListDate],[coef]"
    df = pd.read_sql_query(query, con=conn)
    return df

def get_Action(start,end,articule, filial,conn):
    query = f"SELECT [PriceListDate],[PriceListEndDate],[coef] FROM [InventorySimul].[dbo].[MarketProgramCoef]\
            where  [PriceListDate]<={end} and [PriceListEndDate]>={start}\
                and [FilID]={filial} and [LagerID]={articule} and [coef]<>1"
    df = pd.read_sql_query(query, con=conn)
    return df

def get_msp(date, filid,lagerid, conn):
    sql=f"exec [etl].[proc_getMSPbyDate] ?,?,?"
    df=pd.read_sql_query(sql,params=(date,filid,lagerid,),con=conn)
    if len(df)==0:
        return 1
    else:
        return df.values.item()

def get_msp_all(start,end,filid,lagerid,conn):
    sql=f"exec [etl].[proc_getMSPbyPeriod] ?,?,?,?"
    df=pd.read_sql_query(sql,params=(start,end,filid,lagerid,),con=conn)
    if len(df)==0:
        return 1
    else:
        return df
