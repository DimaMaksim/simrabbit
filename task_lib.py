import sys
import requests
import warnings
warnings.filterwarnings("ignore")
import numpy as np
import pandas as pd
import json


apollo_url = 'http://s-kv-center-x14.officekiev.fozzy.lan:7100'
aoengine_url = 'http://10.10.16.217:7004/process'
class fifo():
    def __init__(self, gdays):
        self.gdays=gdays
        self.inside=[]
        self.off=0
    def addsup(self,h):
        self.off=0
        self.inside.append([h,1])
    def process(self,subtr):
        self.off=0
        s=subtr;k=0
        m=len(self.inside)
        while k<m:
            s=self.inside[k][0]-s
            if s>0:
                self.inside[k][0]=s
                break
            else:
                self.inside[k][0]=s
                k+=1;s=-s

        self.inside=[g for g in self.inside if g[0]>0 ]
        for g in self.inside[:]:
            g[1]+=1
            if g[1]>=self.gdays+1:
                self.off=g[0]
                self.inside.remove(g)
class Gvidon:
    def __init__(self,return_dict,params):
        for k,v in params.items():
            exec(f'self.{k}=v')

        self.DAYS=self.DaysForHistory+self.DaysForSigmaSS
        self.return_dict=return_dict
        self.START=pd.to_datetime(self.StartDate).date()
        self.START_2minus=self.START-pd.to_timedelta(self.DAYS,unit='d')
        self.START_minus=self.START-pd.to_timedelta(self.DaysForSigmaSS,unit='d')
        self.END=pd.to_datetime(self.EndDate).date()
        self.END_plus=self.END+pd.to_timedelta(15,unit='d')
        self.START_2minus_str="\'"+str(self.START_2minus)+"\'"
        self.END_plus_str="\'"+str(self.END_plus)+"\'"
        self.prepare_init_method_count=0






    def get_method_name(self):
        methods=json.loads(requests.get(f'{apollo_url}/params').content)
        self.method_name=next(iter(self.PredictionMethod))
        method_params={}
        for aa,bb in list(zip(methods[self.method_name],self.PredictionMethod[self.method_name])):
            method_params[aa]=bb
        self.method_params = json.dumps(method_params)



    def prepare_aoengine_method(self,i0,i1,dlina=0):
        def prepare_Engine_predict(InputDict,DaysNum):
            url=f'{aoengine_url}/{self.method_name}'
            #url=f'http://10.10.15.86:8080/env/1a5/apps/stacks/1st91/services/1s504/containers/{EngineName}'

            re=self.session.post(url,json=InputDict).json()
            if DaysNum<=7:
                return re[:DaysNum]
            elif DaysNum<=14:
                l_=InputDict["Data"]
                last_date=l_[len(l_)-1]["Date"]
                dates_to_add=list(map(lambda x:str(x.date()),pd.date_range(last_date,periods=8)))[1:]
                old_dates=[x["Date"] for x in l_]
                old_qty=[x["Qty"] for x in l_]
                old_isnulsales=[x["IsNullSales"] for x in l_]
                Dates=old_dates[7:]+dates_to_add
                Gaps=old_isnulsales[7:]+[1 if x==0 else 0 for x in re]
                Qty=old_qty[7:]+re
                l0=list(zip(Dates,Qty,Gaps))
                l=[{"Date":x[0],"Qty":x[1],"IsNullSales":x[2]} for x in l0]
                InputDict=dict(Data=l,ForecastDate=str(pd.to_datetime(last_date)+pd.DateOffset(1)),classId=int(self.classid))
                new_re=requests.post(url,json=InputDict).json()
                return (re+new_re)[:DaysNum]

        def history_for_4weeks(date,df):
            def getwd(a,k):
                ar=a[:,::-1][:,k-1::7]
                b=ar[:,(ar[0,:]!=0)&(ar[3,:]==0)][:,:4]
                m=b.shape[1]
                c=np.pad(b,[(0,0),(0,max(0,4-m))],constant_values=(0))
                return c

            def inst(a1,a2,n):
                ind=list(range(n-1,a1.shape[1]+1,n-1))
                d=np.insert(a1,ind,a2,axis=1)
                return d
            basesales=df[["NonZeroSS","quantity","gaps","promo_days"]][pd.to_datetime(df.Date.values)<date].values.T

            a=[getwd(basesales,x) for x in range(1,8)]
            w2=inst(a[0],a[1],2)
            w3=inst(w2,a[2],3)
            w4=inst(w3,a[3],4)
            w5=inst(w4,a[4],5)
            w6=inst(w5,a[5],6)
            w7=inst(w6,a[6],7)
            return w7[:,::-1]

        def to_date(x):
            return str(x.date())

        ar_date=np.vectorize(to_date,otypes=[str])
        if int(i1-i0)>14:
            self.return_dict["out"]=dict(error=str([self.Articule,self.Filid,"Prediction period for Baseline is more than 14 days"]),traceback="")
            sys.exit()
        if self.UsePrevWeekDaysforBaseline=="Y":
            Date=list(ar_date(self.frame[i0-28:i0,0]))
            #if self.EngineName in ("AOEngine01","AOEngine02","AOEngine03","AOEngine04"):
            history=history_for_4weeks(self.frame[i0,0],self.histSS)
            Qty=list(history[1,:])
            Gaps=list(history[2,:])
            #else:
               # Qty=list(self.frame[i0-28:i0,16])
             #   Gaps=isnullsales[i0-28:i0]
        elif self.UsePrevWeekDaysforBaseline=="N":
            mask_promo=self.frame[i0-28:i0,9]==0
            Date=list(ar_date(self.frame[i0-28:i0,0][mask_promo]))
            Qty=list(self.frame[i0-28:i0,16][mask_promo])
            Gaps=[1 if q==0 else 0 for q in Qty ]
           #Gaps_=isnullsales[i0-days_shift:i0]
        l0=list(zip(Date,Qty,Gaps))
        l=[{"Date":x[0],"Qty":x[1],"IsNullSales":x[2]} for x in l0]
        InputDict=dict(Data=l,ForecastDate=str(self.frame[i0,0].date()),classId=int(self.classid))
        sales_pred=prepare_Engine_predict(InputDict=InputDict,DaysNum=int(i1-i0))

        return sales_pred


    def prepare_strait_method(self,i0,i1,dlina=0):

        data_for_prediction={}
        data_for_prediction['days_forward'] = int(i1-i0)
        data_for_prediction['sales'] = list(self.frame[i0-self.DaysForHistory:i0,16])
        data_for_prediction['promo_days'] = list(self.frame[i0-self.DaysForHistory:i0,9])
        post_content={}
        post_content["data_for_prediction"]=data_for_prediction
        response = requests.get(f'{apollo_url}/predict/{self.method_name}/{self.method_params}',
                                    json=json.dumps(post_content))
        sales_pred = json.loads(response.content)
        return sales_pred

    def test(self,i0,i1,dlina=0):
        return [0]*(i1-i0)
    def prepare_init_method(self,i0,i1,dlina):
        def to_date(x):
            return str(x.date())



        def get_predictions_init():
            def history_for_apollo():
                if (self.SalesTransformationForSimulation=="NoOutOfStockMitigation"):
                    sal="quantity"
                elif (self.SalesTransformationForSimulation=="OutOfStockMitigation"):
                    sal="filt_quantity"
                mask=(self.df_all.Date<self.START_minus)

                self.df_for_apollo=self.df_all[["Date",sal,"returns","StoreQtyDefault","promo_days","PriceOut"]][mask]
                self.df_for_apollo.rename(columns={"StoreQtyDefault":"residue","PriceOut":"price",sal:"sales","Date":"days"},inplace=True)
                self.df_for_apollo_json=self.df_for_apollo.to_json()

            history_for_apollo()
            data_for_init=self.df_for_apollo_json

            response=self.session.post(f'{apollo_url}/init/{self.method_name}/{self.method_params}',json=data_for_init)
            self.params_structure = json.loads(response.content)

        def get_predictions_next(start_date,n,history_dates,history_sales,history_resid,history_price,history_promo):
            data_for_prediction={}
            data_for_prediction['day_prognose_starts'] = start_date
            data_for_prediction['days_forward'] = n
            data_for_prediction['days'] = history_dates
            data_for_prediction['sales'] = history_sales
            data_for_prediction['residue'] = history_resid
            data_for_prediction['price'] = history_price
            data_for_prediction['promo_days'] = history_promo
            data_for_prediction['new_price']=None
            #data_for_prediction = json.dumps(data_for_prediction)
            post_content = {}
            post_content['params_struct'] = self.params_structure
            post_content['data_for_prediction'] = data_for_prediction
            response = self.session.post(f'{apollo_url}/predict/{self.method_name}/{self.method_params}',json=json.dumps(post_content))
            response_content=json.loads(response.content)
            self.params_structure=response_content["params_struct"]

            return response_content["prediction"]

        def prepare_all():

            ar_date=np.vectorize(to_date,otypes=[str])
            history_dates=list(ar_date(self.frame[i0-dlina:i0,0]))
            history_sales=list(self.frame[i0-dlina:i0,16])
            history_resid=list(self.frame[i0-dlina:i0,5])
            history_price=list(self.frame[i0-dlina:i0,6])
            history_promo=list(self.frame[i0-dlina:i0,9])
            resp_next=get_predictions_next(start_date=str(self.frame[i0,0].date()),n=int(i1-i0),\
                        history_dates=history_dates,history_sales=history_sales,\
                        history_resid=history_resid,history_price=history_price,history_promo=history_promo)

            return resp_next

        self.prepare_init_method_count+=1
        if self.prepare_init_method_count==1:
            history_dates,history_sales,history_resid,history_price,history_promo=[],[],[],[],[]
            get_predictions_init()

        sales_pred=prepare_all()
        return sales_pred

    def predict(self):
        def make_frame():
            cols=('msp','SS','Stock','Supply_order','Supply','SalesSimul','sales_pred','off')
            for col in cols:
                self.df_all[col]=np.zeros(self.n,dtype=np.float32)

            if (self.SalesTypeForForecast=="NoFiltered")and(self.SalesTransformationForSimulation=="NoOutOfStockMitigation")and(self.ApplyMPCoeff=="Y"):
                sal_for_stock,sal_for_history="quantity","quantity_coef"
            elif (self.SalesTypeForForecast=="NoFiltered")and(self.SalesTransformationForSimulation=="NoOutOfStockMitigation")and(self.ApplyMPCoeff=="N"):
                sal_for_stock,sal_for_history="quantity","quantity"
            elif (self.SalesTypeForForecast=="Filtered")and(self.SalesTransformationForSimulation=="NoOutOfStockMitigation")and(self.ApplyMPCoeff=="Y"):
                sal_for_stock,sal_for_history="filt_quantity","quantity_coef"
            elif (self.SalesTypeForForecast=="Filtered")and(self.SalesTransformationForSimulation=="NoOutOfStockMitigation")and(self.ApplyMPCoeff=="N"):
                sal_for_stock,sal_for_history="filt_quantity","quantity"
            elif (self.SalesTypeForForecast=="Filtered")and(self.SalesTransformationForSimulation=="OutOfStockMitigation"):
                sal_for_stock,sal_for_history="filt_quantity","filt_quantity"
            elif (self.SalesTypeForForecast=="NoFiltered")and(self.SalesTransformationForSimulation=="OutOfStockMitigation"):
                sal_for_stock,sal_for_history="quantity","filt_quantity"

            frame_cols=["Date",sal_for_stock,"delivery_shedule","order_shedule","sales_pred",\
                "StoreQtyDefault","PriceOut","returns","SS","promo_days",\
                "Stock","Supply_order","SalesSimul","Supply","msp",\
                "mpcoef",sal_for_history,"off","Msp"]
            self.frame=self.df_all[frame_cols].values

        def save_predictions(i0,i1,frame,first_time):
            frame[i0:i1,4]=sales_pred
            if first_time==1:
                frame[i0:i1,12]=sales_pred
            if self.PromoProcessing=="Deactive":
                mask_sal=frame[i0:i1,9]==1
                frame[i0:i1,4][mask_sal]=frame[i0:i1,1][mask_sal]
                frame[i0:i1,12][mask_sal]=frame[i0:i1,1][mask_sal]
            return frame
        def safetystock_delivery(i0,i1,first_time,mm,k):
            mask_dev=(self.frame[i0-self.DaysForSigmaSS:i0,9]==0)
            dev=(self.frame[i0-self.DaysForSigmaSS:i0,4]-np.maximum(self.frame[i0-self.DaysForSigmaSS:i0,16]-self.frame[i0-self.DaysForSigmaSS:i0,7],0))[mask_dev]
            msp_=self.frame[i0,18]
            #msp_=get_msp(str(self.frame[i0,0]).split(" ")[0].replace("-",""),self.Filid,self.Articule,conn_msp)

            if self.SafetyStock=="calculated":
                ss=round(dev.std()*self.z*np.sqrt(self.T+self.L)+msp_,2) if len(dev)!=0 else msp_
            else:
                ss=self.SafetyStock+msp_

            self.frame[k,8]=ss
            self.frame[i0:i1,14]=msp_


            if first_time==1:
                delta=min(self.InitialStock,self.frame[k,4])
                if self.FirstSupply==0:
                    sup=max(0,ss+delta+sum(self.frame[k+1:i1,4])-self.InitialStock) if mm==1 else 0
                else:
                    sup=max(0,ss+delta+sum(self.frame[k+1:i1,4])-self.InitialStock)
                sup=(sup+self.Rasf-sup%self.Rasf if sup%self.Rasf!=0 else sup)
                self.frame[k,11]=sup
                self.frame[k,13]=sup
                init=self.InitialStock
                OFF.addsup(sup+self.InitialStock)
                for f,t in enumerate(range(k,i1)):
                    if f==0:
                        init=self.InitialStock+self.frame[t,13]
                    else:
                        init=max(init+self.frame[t,13]-self.frame[t-1,1],0) if init-self.frame[t-1,1]>0 else self.frame[t,13]
                    self.frame[t,10]=round(init,2)
                    self.frame[t,12]=min(self.frame[t,1],init)

                    if init==0:
                        self.frame[t,12]=min(self.frame[t,1],self.frame[t,13])
                    OFF.process(self.frame[t,12])
                    self.frame[t,17]=OFF.off
                    if self.Offs=="Y":
                        self.frame[t,10]=round(init,2)-OFF.off
                        init-=OFF.off



            else:
                delta=delta=min(self.frame[i0,10],self.frame[i0,4])
                sup=max(0,ss+delta+sum(self.frame[i0+1:i1,4])-self.frame[i0,10]) if k-self.L>=self.DAYS else 0

                sup=(sup+self.Rasf-sup%self.Rasf if sup%self.Rasf!=0 else sup)
                self.frame[i0,11]=sup
                self.frame[k,13]=sup
                init=init0
                OFF.addsup(sup)
                for f,t in enumerate(range(k,i1)):
                    init=max(init+self.frame[t,13]-self.frame[t-1,1],0) if init-self.frame[t-1,1]>0 else self.frame[t,13]
                    self.frame[t,10]=round(init,2)
                    self.frame[t,12]=min(self.frame[t,1],init)

                    if init==0:
                        self.frame[t,12]=min(self.frame[t,1],self.frame[t,13])
                    OFF.process(self.frame[t,12])
                    self.frame[t,17]=OFF.off
                    if self.Offs=="Y":
                        self.frame[t,10]=round(init,2)-OFF.off
                        init-=OFF.off

            return init


        make_frame()
        self.session=requests.Session()
        #isnullsales=[1 if x==0 else 0 for x in self.frame[:,16]]
        mm=self.frame[self.DAYS,2]
        self.frame[self.DAYS,2]=1
        ind=self.frame[:,2].nonzero()[0]
        st=np.argwhere(ind==self.DAYS).item()

        p=0
        while ind[p]-self.L<0:
            p+=1


        init=self.InitialStock
        massiv=ind[p:]
        dlina=massiv[1]-massiv[0]
        massive_len=len(massiv)
        massive_numerator=enumerate(massiv)

        OFF=fifo(self.godnost)
        if self.PredictionMethod in ["BaseLine","AOEngine01","AOEngine02",\
                                        "AOEngine03","AOEngine04","AOEngine05","AOEngine03Seasonal"]:
            forecast=self.prepare_aoengine_method
        elif self.PredictionMethod=="test":
            forecast=self.test
        else:
            self.get_method_name()
            if self.method_name in ["4weeks","4_analogous_days","14_prev_days","mixed_nn"]:
                forecast=self.prepare_strait_method
            else:
                forecast=self.prepare_init_method


        for j,k in massive_numerator:
            if j<massive_len-2:

                i0=int(k-self.L); i1=int(massiv[j+1])
                 #mask=df_restr[i0-dlina:i0,9]==0
                if (k>=self.DaysForHistory)and(k<self.DAYS)&(i0-self.DaysForHistory>0):
                    sales_pred=forecast(i0,i1,dlina)

                    self.frame=save_predictions(i0,i1,self.frame,1)
                    dlina=i1-k


                elif k==self.DAYS:
                    sales_pred=forecast(i0,i1,dlina)
                    self.frame=save_predictions(i0,i1,self.frame,0)

                    dlina=i1-k

                    init0=safetystock_delivery(i0,i1,1,mm,k)

                elif k>self.DAYS:

                    sales_pred=forecast(i0,i1,dlina)
                    self.frame=save_predictions(i0,i1,self.frame,0)

                    dlina=i1-k
                    init0=safetystock_delivery(i0,i1,0,mm,k)


        self.session.close()

        c=["msp","SS","sales_pred","Stock","Supply_order","SalesSimul","Supply","off"]
        pd_frame=pd.DataFrame(data=self.frame[:,[14,8,4,10,11,12,13,17]],columns=c)
        self.df_all=self.df_all.assign(**pd_frame)
        self.df_all["dev"]=self.frame[:,4]-np.maximum(self.frame[:,1]-self.frame[:,7],0)



    def good_names(self):
        def fill_zeros(l):
            x=0
            for i in range(len(l)):
                if l[i]!=0:
                    x=l[i]
                else:
                    l[i]=x
            return l
        self.df=self.df_all[["Date","order_shedule","delivery_shedule","quantity","StoreQtyDefault",\
                "promo_days","AmountSales","sales_pred","SS","Stock","Supply","Supply_order","SalesSimul",\
                    "PriceOut","dev","returns", "AmountSales","StoreAmountDefault","msp","NonZeroSS","off"]]
        self.df=self.df[(self.df.Date>=self.START) & (self.df.Date<=self.END)]
        columns={"quantity":"SalesQuantReal","returns":"ReturnsQuantReal","AmountSales":"SalesAmounReal","StoreQtyDefault":"StockQuantReal","StoreAmountDefault":"StockAmountReal",\
                "promo_days":"IsPromoDay","delivery_shedule":"DeliveShed","order_shedule":"OrderShedule","sales_pred":"SalesQuantPredict","SS":"SafetyStockSimul","Supply":"SupplyDeliv","Supply_order":"SupplyOrder"}
        self.df.rename(columns=columns,inplace=True)

        #недопродажи
        def UnSales(x):
            if x.Stock<x.SalesQuantReal:
                if x.Stock==0:
                    return round(max(0,x.SalesQuantReal-x.SalesSimul),2)
                else:
                    return round(max(0,-x.Stock+x.SalesQuantReal),2)
        self.df["UnderSales"]=self.df.apply(UnSales,axis=1).fillna(0)

        # week turnover
        self.df["week"]=self.df.Date.dt.week

        df_temp=self.df[["Stock","SalesSimul","StockQuantReal","SalesQuantReal","week"]]

        df_temp.SalesSimul=pd.to_numeric(df_temp.SalesSimul)
        df_temp.StockQuantReal=pd.to_numeric(df_temp.StockQuantReal)
        df_temp.SalesQuantReal=pd.to_numeric(df_temp.SalesQuantReal)
        df_temp.Stock=pd.to_numeric(df_temp.Stock)
        df_temp=df_temp.groupby(["week"]).mean()

        df_temp["TurnOverSimul"]=np.around(df_temp.Stock.values/df_temp.SalesSimul.values,2)
        df_temp["TurnOverReal"]=np.around(df_temp.StockQuantReal.values/df_temp.SalesQuantReal.values,2)
        self.df=self.df.merge(df_temp,how='left',on='week')

        self.df.drop(["SalesQuantReal_y","Stock_y","SalesSimul_y","StockQuantReal_y"],axis=1,inplace=True)

        self.df.rename({"SalesQuantReal_x":"SalesQuantReal","Stock_x":"Stock","TurnOverSimul_x":"TurnOverSimul","TurnOverReal_x":"TurnOverReal",\
                         "StockQuantReal_x":"StockQuantReal","SalesSimul_x":"SalesSimul"},\
                           axis=1,inplace=True)
        self.df.Date=self.df.Date.dt.date
        self.df.SafetyStockSimul=fill_zeros(self.df.SafetyStockSimul.values)
        df_details=self.df.drop(["dev","week","StockAmountReal",'PriceOut','ReturnsQuantReal','SalesAmounReal'],axis=1)
        #df_details.SalesQuantPredict=np.around(df_details.SalesQuantPredict.values.astype(float),3)
        #df_details.SalesSimul=np.around(df_details.SalesSimul.values.astype(float),3)
        rus_headers={"Date":"Дата","OrderShedule":'Заказ','DeliveShed':'Поставка','SalesQuantReal':'ПродРеал',\
              'StockQuantReal':'ОстРеал','IsPromoDay':'Промо','SalesQuantPredict':'ПродПрогноз','SafetyStockSimul':'СтрахЗапас',\
             'Stock':'ОстСимул','SupplyDeliv':'ПоставкаОбъем','SupplyOrder':'ЗаказОбъем',\
              'SalesSimul':'ПродСимул','msp':'msp','UnderSales':'НедоПрод','TurnOverReal':'ОборачРеал','TurnOverSimul':'ОборачСимул','DelivReal':'ПоставкаРеал','DelivRealRaw':'ПостРеалБезфильт'}
        #df_details.rename(columns=rus_headers,inplace=True)
        return df_details
    def fmetrics(self):
        eps=3

        StockQuantRe=self.df.StockQuantReal.values
        SalesQuantReal=self.df.SalesQuantReal.values
        self.df.PriceOut.fillna(self.df.PriceOut.mean(),inplace=True)
        DaysNum=len(self.df)
        m_realSalesStock=DaysNum-self.df.NonZeroSS.sum()
        RealDeliv=StockQuantRe[1:]-StockQuantRe[:-1]+SalesQuantReal[:-1]
        self.df.loc[1:,"DelivReal"]=np.where(RealDeliv<self.Rasf,0,RealDeliv)
        self.df.loc[1:,"DelivRealRaw"]=RealDeliv

        zz=np.nonzero(SalesQuantReal)
        df=self.df.loc[max(0,zz[0][0]-1):min(DaysNum,zz[0][-1]+1)].fillna(0)
        df=df.loc[df.NonZeroSS==1]
        StockQuantRe=df.StockQuantReal.values
        SalesQuantReal=df.SalesQuantReal.values
        SalesDiff=round(np.absolute(np.diff(SalesQuantReal)).mean(),eps)
        Variance=round(SalesQuantReal.std()/SalesQuantReal.mean(),eps)
        N=len(df)
        dev=df.dev.values
        msp=round(df.msp.values.mean(),eps)
        SalesQuantSimul_mean=round(df.SalesSimul.values.mean(),eps)
        StockAvailab=round(100*(df.Stock.values>0).mean(),eps)
        StockAvailab_msp=round(100*(df.Stock.values>df.msp.values).mean(),eps)
        SalesAvail=round(100*(1-df["UnderSales"][df["UnderSales"]>0].values.sum()/SalesQuantReal.sum()),eps)

        SalesQuantReal_mean=round(SalesQuantReal.mean(),eps)
        SalesCostReal=round(df.SalesAmounReal.values.mean(),eps)
        StockAvailReal=round(100*(StockQuantRe>0).mean(),eps)
        StockAvailReal_msp=round(100*(StockQuantRe>df.msp.values).mean(),eps)
        StockQuantSimul=df.Stock.fillna(0).values
        StockQuantReal_mean=round(StockQuantRe.mean(),eps)
        RealDeliv=StockQuantRe[1:]-StockQuantRe[:-1]+SalesQuantReal[:-1]
        DelivQuantReal=round(RealDeliv[RealDeliv>=self.Rasf].sum(),2)
        DelivQuantRealDays=RealDeliv[RealDeliv>=self.Rasf].size
        DelivQuantSimul=float(round(df.SupplyDeliv.values.sum(),2))
        DelivQuantSimulDays=df.SupplyDeliv[df.SupplyDeliv>0].size
        mask=(StockQuantRe!=0)

        OutStockQuant_mean=round(df.UnderSales.values.mean(),eps)
        OutStockCost_mean=round((df.UnderSales.values*df.PriceOut.values).mean(),eps)

        StockAmountRe=df.StockAmountReal.values
        PriceMedian=np.median(df.PriceOut.values)
        PriceIn=StockAmountRe/StockQuantRe
        Margin=round(np.nansum(((df.PriceOut.values[mask]-PriceIn[mask])*(df.SalesSimul.values[mask]))),eps)
        MarginReal=round(np.nansum(((df.PriceOut.values[mask]-PriceIn[mask])*(df.SalesQuantReal.values[mask]))),eps)
        StockQuant_mean=round(np.nanmean(StockQuantSimul),eps)
        StockCost_mean=round((PriceIn[mask]*df.Stock[mask]).mean(),eps)
        StockCostReal_mean=round((StockAmountRe[mask]).mean(),eps)
        OffsQuant_mean=round(df.off.values.mean(),eps) if self.Offs=="Y" else 0
        OffsCost_sum=round(((PriceIn[mask])*df.off.values[mask]).sum(),eps) if self.Offs=="Y" else 0

        SafetyStock_mean=round(df.SafetyStockSimul.values.mean(),eps)
        SafetyStockCost_mean=round((df.SafetyStockSimul.values[mask]*PriceIn[mask]).mean(),eps)
        Bias=round(np.abs(dev).mean()/np.abs(dev).std(),3)
        RMSE_money=round(np.sqrt((((df.dev.values*df.PriceOut.values)[df.IsPromoDay.values!=1])**2).mean()),eps)
        RMSE_quant=round(np.sqrt(((df.dev.values[df.IsPromoDay.values!=1])**2).mean()),eps)
        WAPE=round(np.abs(df.dev.values[df.IsPromoDay.values!=1]).sum()/SalesQuantReal[df.IsPromoDay.values!=1].sum(),eps)

        StockDif_mean=round(np.nanmean(StockQuantRe-StockQuantSimul),eps)
        StockDifKoef=round(StockDif_mean/StockQuantRe.mean(),eps)
        StockDif_max=round(np.nanmax(StockQuantRe-StockQuantSimul),eps)
        PromoDays=round(100*df.IsPromoDay.values.sum()/N,eps)
        metrics_finance=dict(StartDate=self.START,EndDate=self.END,ZeroSalesAndStock=m_realSalesStock,SimulDays=N,Articule=self.Articule,Filid=self.Filid,PredictionMethod=self.PredictionMethod,DaysNum=DaysNum,PercentDaysPromo=PromoDays,MarginSimul_sum=Margin,StockCostSimul_mean=StockCost_mean,SalesQuantReal_mean=SalesQuantReal_mean,Rasf=self.Rasf,
                        SafetyStock_mean=SafetyStock_mean,SSL=self.SSL,Bias=Bias,RMSECost=RMSE_money,RMSEQuant=RMSE_quant,StockDif_mean=StockDif_mean,StockDif_max=StockDif_max,
                        StockAvailSimul=StockAvailab,SalesAvailSimul=SalesAvail,StockDifKoef=StockDifKoef,SafetyStockCost_mean=SafetyStockCost_mean,StockQuantSimul_mean=StockQuant_mean,SalesQuantSimul_mean=SalesQuantSimul_mean,\
                        DelivQuantReal_sum=DelivQuantReal,DelivQuantRealDays_sum=DelivQuantRealDays,DelivQuantSimul_sum=DelivQuantSimul,DelivQuantSimulDays_sum=DelivQuantSimulDays,\
                        PriceOutMedian=PriceMedian,StockQuantReal_mean=StockQuantReal_mean,MarginReal_sum=MarginReal,StockAvailReal=StockAvailReal,StockAvailReal_msp=StockAvailReal_msp,StockAvailSimul_msp=StockAvailab_msp,\
                        StockCostReal_mean=StockCostReal_mean,SalesCostReal=SalesCostReal,L=int(self.L),T=self.T,OffsQuant_mean=OffsQuant_mean,OffsCost_sum=float(OffsCost_sum),\
                        SalesDiff=SalesDiff,VarianceRealSales=Variance,ShelfLife=self.godnost,Msp_mean=msp,OutStockQuant_mean=OutStockQuant_mean,OutStockCost_mean=OutStockCost_mean,\
                        WAPE=WAPE)


        for u,v in metrics_finance.items():
            if hasattr(v, 'dtype'):
                metrics_finance[u]=v.item()
        return metrics_finance
