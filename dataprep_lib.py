import numpy as np
import pandas as pd
import json
import sys
import pickle
from scipy.stats import norm
from Connection import *
import yaml
with open("dataprep_config.yaml") as f:
    config=yaml.full_load(f)

def errorproc(message,dict):
    dict.update({'out':message})
    return dict

class Dataprep:
    def __init__(self,params):
        for k,v in params.items():
            exec(f'self.{k}=v')

        self.return_dict={}
        self.START=pd.to_datetime(self.StartDate).date()
        self.DAYS=self.DaysForHistory+self.DaysForSigmaSS
        self.START_2minus=self.START-pd.to_timedelta(self.DAYS,unit='d')
        self.START_minus=self.START-pd.to_timedelta(self.DaysForSigmaSS,unit='d')
        self.END=pd.to_datetime(self.EndDate).date()
        self.END_plus=self.END+pd.to_timedelta(15,unit='d')
        self.START_2minus_str="\'"+str(self.START_2minus)+"\'"
        self.END_plus_str="\'"+str(self.END_plus)+"\'"
        self.DAYS=self.DaysForHistory+self.DaysForSigmaSS

        self.driver=config['driver']
        self.server27=config['sales']['servername']
        self.uid0=config['sales']['user']
        self.pwd0=config['sales']['pwd']

        try:
            self.conn_sales=pyodbc.connect(driver=self.driver,server=self.server27,database=config['sales']['dbname'],uid=self.uid0,pwd=self.pwd0)
        except Exception:
            self.return_dict["out"]=dict(error=str([self.Articule,self.Filid,"Unnable to connect to SalesHub.Dev"]),traceback="")
            return
    def simulationpossibility(self):
        #проверка фильтрованых:
        if self.SalesTransformationForSimulation=="OutOfStockMitigation" or self.SalesTypeForForecast=="Filtered":
            self.f_sales=get_filter_sales(self.Articule, self.Filid, self.START_2minus_str,self.END_plus_str,self.conn_sales)
            f_sales_values=self.f_sales.values
            v_dat=f_sales_values[:,0]
            v1=(v_dat>=self.START)&(v_dat<=self.END)
            v2=(v_dat>=self.START_minus)&(v_dat<=self.END)
            for v,ans in zip((v1,v2),("Sales","HistorySales")):
                if np.nansum(f_sales_values[v][:,1])==0:
                    self.return_dict["out"]=dict(error=str([self.Articule,self.Filid,f"Filtered {ans} for defined period is empty"]),traceback="")
                    self.conn_sales.close()
                    return


        #проверка обычных:
        (val,col) = get_sales(self.Articule, self.Filid, self.START_2minus_str,self.END_plus_str,self.conn_sales)
        val[val==None]=0
        v_dat=val[:,0]
        v0=val[:,[1,5]]
        v1=(v_dat>=self.START)&(v_dat<=self.END)
        v2=(v_dat>=self.START_minus)&(v_dat<=self.END)
        for v in (v1,v2):
            if np.nansum(v0[v][:,0])==0:
                mes=str([self.Articule,self.Filid,"(History) Sales for defined period is empty"])
                self.return_dict=errorproc(mes,self.return_dict)
                #self.return_dict["out"]=dict(error=str([self.Articule,self.Filid,"(History) Sales for defined period is empty"]),traceback="")
                print(self.return_dict)
                self.conn_sales.close()
                return

            if np.nansum(v0[v][:,1])==0:
                self.return_dict["out"]=dict(error=str([self.Articule,self.Filid,"(History) Stock for defined period is empty"]),traceback="")
                self.conn_sales.close()
                return
        val[:,5][val[:,5]<0]=0
        val[:,0]=pd.to_datetime(val[:,0])
        dat=pd.DataFrame({'Date':pd.date_range(self.START_2minus,self.END_plus)})
        self.df_all=pd.DataFrame(data=val,columns=col)
        self.df_all=pd.merge(dat,self.df_all,how='left',on='Date')

        if self.SalesTransformationForSimulation=="OutOfStockMitigation" or self.SalesTypeForForecast=="Filtered":
            self.f_sales.Date=pd.to_datetime(self.f_sales.Date.values)
            self.df_all=self.df_all.merge(self.f_sales,how='left',on='Date')

        self.df_all.fillna(0,inplace=True)
        self.df_all.loc[:,"NonZeroSS"]=(self.df_all.quantity.values>0)|(self.df_all.StoreQtyDefault.values>0)
    def order_shedule(self):
        try:
            conn_MD=pyodbc.connect(driver=self.driver,server=config['order_shed']['servername'],database=config['order_shed']['dbname'],uid=config['order_shed']['user'],pwd=config['order_shed']['pwd'])
        except Exception:
            self.return_dict["out"]=dict(error=str([self.Articule,self.Filid,"Unnable to connect to MD"]),traceback="")
            return
        self.n=len(self.df_all)
        if self.DeliveryShedule=="BaseLine":

            d_dates=deliv_shedule(self.Articule,self.Filid,self.START_2minus,self.END_plus,conn_MD)

            if len(d_dates)==0:
                #conn_MD.close()
                #return_dict["out"]=dict(error=str([Articule,Filid,"Unable to find OrderShedule"]),traceback="")
                #sys.exit()

                self.df_all["order_shedule"]=1
            else:
                self.df_all=self.df_all.merge(d_dates,how="left",left_on="Date",right_on="date")
                self.df_all.date=self.df_all.date.apply(lambda x: 0 if pd.isnull(x) else 1)
                self.df_all.rename({'date':'order_shedule'},axis=1,inplace=True)
        else:
            dates=self.df_all.Date
            w=self.df_all.Date.apply(lambda x:x.dayofweek+1).values
            fdw=w[self.DAYS]
            d=self.DeliveryShedule[0]
            k=self.DeliveryShedule[1]
            shed=d*(self.n//14+2)
            c=14-(self.DAYS-fdw+1)%14 if k==1 else 14-(self.DAYS-fdw-6)%14
            self.df_all.loc[:,"order_shedule"]=shed[c:self.n+c]


        try:
            self.T,self.L=av_period(self.Articule,self.Filid,conn_MD)
        except:
            conn_MD.close()
            return_dict["out"]=dict(error=str([self.Articule,self.Filid,"Unable to get T or L from MD"]),traceback="")
            sys.exit()
        if self.LeadTime!="BaseLine":
            self.L=self.LeadTime
        conn_MD.close()
        if self.L<=0:
            return_dict["out"]=dict(error=str([self.Articule,self.Filid,"Uncorrect Leadtime"]),traceback="")
            sys.exit()
        ar_help=np.empty(self.L,dtype=int);ar_help.fill(0)
        ar=np.append(ar_help,self.df_all.order_shedule.values[:-self.L])
        self.df_all.loc[:,"delivery_shedule"]=ar
    def mpcoefficients_godnost(self):
        self.histSS=self.df_all[["Date","quantity","NonZeroSS","promo_days"]]
        self.histSS.loc[:,"gaps"]=(self.histSS.quantity.values==0)
        try:
            conn_invent=pyodbc.connect(driver=self.driver,server=config['mpcoef']['servername'],database=config['mpcoef']['dbname'],uid=config['mpcoef']['user'],pwd=config['mpcoef']['pwd'])
        except Exception:
            self.return_dict["out"]=dict(error=str([self.Articule,self.Filid,"Unnable to connect to InventorySimul"]),traceback="")
            return
        # срок годности:
        self.godnost=np.ceil(float(getGodnost(self.Articule,conn_invent).values.item()))

        # для SSL:
        self.r=get_ZP(self.Articule,self.Filid,conn_invent)

        mp_prog=get_Action(self.START_2minus_str,self.END_plus_str,self.Articule,self.Filid,conn_invent)
        conn_invent.close()
        mp_prog_sorted=mp_prog.sort_values(by=['PriceListDate']).values
        res_arr=np.empty((0,2),dtype=object)
        k=0
        for row in mp_prog_sorted:
            if k==0:
                dates=pd.date_range(max(self.START_2minus,row[0]),row[1]).values
            elif k<len(mp_prog_sorted)-1:
                dates=pd.date_range(row[0],row[1]).values
            else:
                dates=pd.date_range(row[0],min(self.END_plus,row[1])).values
            coef_arr=np.full_like(dates,row[2],dtype=np.float32)
            for_append=np.array([dates,coef_arr],dtype=object).T
            res_arr=np.append(res_arr,for_append,axis=0)
            k+=1

        df=pd.DataFrame(data=res_arr,columns=['Date','Coef'])
        df.drop_duplicates(inplace=True)
        df.Date=pd.to_datetime(df.Date)

        dates_mp_prog=self.histSS.Date
        df=df.merge(dates_mp_prog,how='right',on='Date')
        df.Coef.fillna(1,inplace=True)
        df.sort_values(by='Date',inplace=True)
        mp_prog_detail=df.Coef.values

        self.histSS.loc[:,"quant_cor"]=self.histSS.quantity.values*mp_prog_detail
        self.df_all.loc[:,'mpcoef']=mp_prog_detail

        self.df_all.loc[:,'quantity_coef']=self.df_all.quantity.values*mp_prog_detail
        if (self.SalesTransformationForSimulation=="OutOfStockMitigation" \
                            or self.SalesTypeForForecast=="Filtered") and (self.ApplyMPCoeff=="Y"):
            self.df_all.loc[:,'filt_quantity_coef']=self.df_all.filt_quantity.values*mp_prog_detail
    def rasfasovka(self):
        if self.Rasf=="BaseLine":
            with open('pallet_5_filials.pickle',"rb") as f:
                palet=pickle.load(f)

            if len(palet.loc[(palet.lagerid==self.Articule)&(palet.filid==self.Filid) ])!=0:
                self.Rasf=palet.loc[(palet.lagerid==self.Articule)&(palet.filid==self.Filid)].rasf.values.item()
            else:
                try:
                    conn_4t=pyodbc.connect(driver=self.driver,server=config['rasf']['servername'],database=config['rasf']['dbname'],uid=config['rasf']['user'],pwd=config['rasf']['pwd'])
                except Exception:
                    self.return_dict["out"]=dict(error=str([self.Articule,self.Filid,"Unnable to connect to 4t.dev"]),traceback="")
                    sys.exit()
                try:
                    self.Rasf=get_rasf(self.Articule,self.Filid,conn_4t)
                    conn_4t.close()
                except:
                    conn_4t.close()
                    self.return_dict["out"]=dict(error=str([self.Articule,self.Filid,"Unnable to connect to get rasfasovka"]),traceback="")
                    sys.exit()

                if len(self.Rasf)==0:
                    self.Rasf=1
                else:
                    if len(self.Rasf.values)==1:
                        self.Rasf=float(self.Rasf.values.item())
                    else:
                        self.Rasf=float(self.Rasf.values[0,0].item())
    def initstock_ssl(self):
        def GetInitStock(Articule,Filid,date,conn):
            df=get_init_store(Articule,Filid,date,conn)
            if len(df)>0:
                return df.values[0,0]
            else:
                return 0
        # начальный запас:
        if self.InitialStock=="BaseLine":
            self.InitialStock=GetInitStock(self.Articule,self.Filid,"\'"+str(self.StartDate)+"\'",self.conn_sales)
            if self.InitialStock==None:
                self.InitialStock=0

        # коэффициенты SSL, z:
        if self.SafetyStock=="calculated":
            if self.SSL=="BaseLine":
                if len(self.r)==0:
                    (z,sl)=(0,50)
                else:
                    r=self.r.values[0]
                    z,sl=float(r[0]),float(r[1])
                    if (z==None)and(sl==None):
                        (z,sl)=(0,50)
                    elif (z!=None)and(sl==None):
                        #z=float(z)
                        sl=norm.cdf(z)*100
                    elif (z==None)and(sl!=None):
                        #sl=float(sl)
                        z=norm.ppf(sl*0.01)
                self.z=z
                self.SSL=sl
            else:
                self.z=norm.ppf(self.SSL)
    def msp(self):
        conn_msp=pyodbc.connect(driver=self.driver\
                            ,server=config['msp']['servername']\
                            ,database=config['msp']['dbname']\
                            ,uid=config['msp']['user']\
                            ,pwd=config['msp']['pwd'])
        msp_df=get_msp_all(self.START,self.END_plus,self.Filid,self.Articule,conn_msp)
        conn_msp.close()
        k=0
        res_arr=np.empty((0,2),dtype=object)
        msp_df_sorted=msp_df.sort_values(by=['dateFrom']).values

        for row in msp_df_sorted:
            if k==0:
                
                dates=pd.date_range(self.START,min(self.END_plus,row[2])).values
            elif k<len(msp_df_sorted)-1:
                dates=pd.date_range(row[1],row[2]).values
            else:
                dates=pd.date_range(row[1],self.END_plus).values
            msp_arr=np.full_like(dates,row[0],dtype=np.float32)
            for_append=np.array([dates,msp_arr],dtype=object).T
            res_arr=np.append(res_arr,for_append,axis=0)
            k+=1

        df=pd.DataFrame(data=res_arr,columns=['Date','Msp'])
        df.Date=pd.to_datetime(df.Date)
        self.df_all=self.df_all.merge(df,how='left',on='Date')
    def choose_baseline_enginename(self):
        if self.PredictionMethod=="BaseLine":

            try:
                conn_ao=pyodbc.connect(driver=self.driver\
                    ,server=config['aoengine']['servername']\
                    ,database=config['aoengine']['dbname']\
                    ,uid=config['aoengine']['user']\
                    ,pwd=config['aoengine']['pwd'])
            except:
                self.return_dict["out"]=dict(error=str([self.Articule,self.Filid,"Unable to connect S-KV-CENTER-S27_AO"]),traceback="")
                return
            try:
                ao_name=get_aoengine_name(self.Articule,self.Filid,conn_ao).values

            except:
                self.return_dict["out"]=dict(error=str([self.Articule,self.Filid,\
                    "Unable to run [AO].[GetForecastTypes] in S-KV-CENTER-S27_AO"]),traceback="")
                return
            finally:
                conn_ao.close()

            if len(ao_name)!=0:
                self.classid=ao_name[0,1]
                self.method_name=ao_name[0,3].strip()

            else:
                self.method_name="AOEngine03"

                conn_lag=pyodbc.connect(driver=self.driver\
                    ,server=config['lagerclass']['servername']\
                    ,database=config['lagerclass']['dbname']\
                    ,uid=config['lagerclass']['user']\
                    ,pwd=config['lagerclass']['pwd'])
                clas=getLagerName(self.Articule,conn_lag)
                self.classid=clas.lagerClassifier.values[0]
                conn_lag.close()
        elif self.PredictionMethod in ("AOEngine01","AOEngine02","AOEngine03","AOEngine04","AOEngine05"):

            conn_lag=pyodbc.connect(driver=self.driver\
                ,server=config['lagerclass']['servername']\
                ,database=config['lagerclass']['dbname']\
                ,uid=config['lagerclass']['user']\
                ,pwd=config['lagerclass']['pwd'])
            clas=getLagerName(self.Articule,conn_lag)
            self.classid=clas.lagerClassifier.values[0]
            conn_lag.close()
            self.method_name=self.PredictionMethod
        else:
            self.classid=-1
            self.method_name='o-o'
