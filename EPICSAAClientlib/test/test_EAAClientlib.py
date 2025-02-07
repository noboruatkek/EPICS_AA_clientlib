#!python3.12
#-*- coding:utf-8 -*-
"""
:Author: noboru.yamamoto@kek.jp
:date:  2025/1/22 - 
"""
import typing
from typing import List, Union

import urllib.request as request ,urllib.parse as parse
import os
import os.path
import asyncio
import sys
import io
import importlib
from importlib import reload
import concurrent.futures

import json,datetime,time
import logging
logging.getLogger().setLevel(logging.WARN)

from EAA_WebAPIs import *

# setup Timezone
# import pytz
# jst=pytz.timezone("Asia/Tokyo")
# import zoneinfo
# JST=zoneinfo.ZoneInfo("Asia/Tokyo")
# UTC=zoneinfo.ZoneInfo("UTC")
JST=datetime.timezone(datetime.timedelta(hours=9))
#UTC=datetime.timezone(datetime.timedelta(hours=0))
UTC=datetime.timezone.utc
SECSADAY=24*3600

#
"""
jーPARC MR用 TEST関数
"""
#
__ai_chan_names=(
    'UTUTL_D1:PWTR:PURITY', 'UTUTL_D1:PWTR:TMP:RET', 'UTUTL_D1:PWTR:PRS:OUT',
    'UTUTL_D1:PWTR:FLOW',
    'UTUTL_D2:PWTR:PURITY', 'UTUTL_D2:PWTR:TMP:RET',
    'UTUTL_D2:PWTR:PRS:OUT',
    'UTUTL_D3:PWTR:PURITY', 'UTUTL_D3:PWTR:TMP:RET',
    'UTUTL_D3:PWTR:PRS:OUT',
    'UTUTL_D1:WTR_RTNLS:ITMP', 'UTUTL_D1:WTR_RTNLE:ITMP',
    'UTUTL_D1:WTR_LTNLS:ITMP', 'UTUTL_D1:WTR_LTNLE:ITMP',
    'UTUTL_D1:WTR_RTNLS:OPRES','UTUTL_D1:WTR_RTNLE:OPRES',
    'UTUTL_D1:WTR_LTNLS:OPRES','UTUTL_D1:WTR_LTNLE:OPRES',
    'UTUTL_D2:WTR_RTNLS:ITMP', 'UTUTL_D2:WTR_RTNLE:ITMP',
    'UTUTL_D2:WTR_LTNLS:ITMP', 'UTUTL_D2:WTR_LTNLE:ITMP',
    'UTUTL_D2:WTR_RTNLS:OPRES', 'UTUTL_D2:WTR_RTNLE:OPRES',
    'UTUTL_D2:WTR_LTNLS:OPRES', 'UTUTL_D2:WTR_LTNLE:OPRES',
    'UTUTL_D3:WTR_RTNLS:ITMP',  'UTUTL_D3:WTR_RTNLE:ITMP',
    'UTUTL_D3:WTR_LTNLS:ITMP',  'UTUTL_D3:WTR_LTNLE:ITMP',
    'UTUTL_D3:WTR_RTNLS:OPRES', 'UTUTL_D3:WTR_RTNLE:OPRES',
    'UTUTL_D3:WTR_LTNLS:OPRES',  'UTUTL_D3:WTR_LTNLE:OPRES',
    'UTUTL_D4:D4-AI046:PURITY',
    'UTUTL_D4:D4-AI047:OTMP',  'UTUTL_D4:D4-AI048:RTMP',
    'UTUTL_D4:D4-AI049:OTMP',
    'UTUTL_D4:D4-AI04A:PRES',  'UTUTL_D4:D4-AI04B:PRES', 'UTUTL_D5:D5-AI066:PURITY',
    'UTUTL_D5:D5-AI067:OTMP', 'UTUTL_D5:D5-AI068:RTMP',
    'UTUTL_D5:D5-AI069:OTMP',
    'UTUTL_D5:D5-AI06A:PRES', 'UTUTL_D5:D5-AI06B:PRES', 'UTUTL_D6:D6-AI040:PURITY',
    'UTUTL_D6:D6-AI041:OTMP', 'UTUTL_D6:D6-AI042:RTMP',
    'UTUTL_D6:D6-AI043:OTMP',
    'UTUTL_D6:D6-AI044:PRES', 'UTUTL_D6:D6-AI045:PRES',
    'UTUTL_D1:WTR_MWTR:TMP',  'UTUTL_D2:WTR_MWTR:TMP', 'UTUTL_D3:WTR_MWTR:TMP',
    'MRRF:WTR_M2:VAL:OHM1', 'MRRF:WTR_M2:VAL:TMP1',
    'MRRF:WTR_M2:VAL:TMP2'
)

__li_chan_names=(
    'UTUTL_D2:PWTR:FLOW', 'UTUTL_D3:PWTR:FLOW', 'UTUTL_D1:WTR_RTNL:FLOW',
    'UTUTL_D1:WTR_LTNL:FLOW', 'UTUTL_D2:WTR_RTNL:FLOW',
    'UTUTL_D2:WTR_LTNL:FLOW', 'UTUTL_D3:WTR_RTNL:FLOW',
    'UTUTL_D3:WTR_LTNL:FLOW', 'UTUTL_D4:D4-AI04C:FLOW',
    'UTUTL_D5:D5-AI06C:FLOW', 'UTUTL_D6:D6-AI046:FLOW'
)

__bi_chan_names=(
    'MRRF:WTR_M2:ILK:CHIL_PMP1_FLT', 'MRRF:WTR_M2:ILK:COOL_PMP1_FLT',
    'MRRF:WTR_M2:ILK:CHIL_PMP2_FLT', 'MRRF:WTR_M2:ILK:COOL_PMP2_FLT',
    'MRRF:WTR_M2:ILK:CHIL_PMPPS_FLT', 'MRRF:WTR_M2:ILK:CIRC_PMP1_FLT',
    'MRRF:WTR_M2:ILK:CIRC_PMP2_FLT', 'MRRF:WTR_M2:ILK:CIRC_PMP3_FLT',
    'MRRF:WTR_M2:ILK:CIRC_PMPPS_FLT', 'MRRF:WTR_M2:ILK:BST_FLT',
    'MRRF:WTR_M2:ILK:BST_PMPPS_FLT',
    'MRRF:WTR_M2:STAT:WTANK_LL', 'MRRF:WTR_M2:STAT:RTANK_LL'
)

__Chan_name= __ai_chan_names + __li_chan_names + __bi_chan_names
    
def test():
    import openpyxl,pandas
    import pandas,numpy
    import math

    xlsxfn="WTA_Channels_Alarm_PB.xlsx"

    now=datetime.datetime.now().astimezone(JST) # EPICS AA requires TZ info.
    df=pandas.DataFrame(
        dict(( (c, getMetadata(c)) for c in __Chan_name))
    ).transpose()
    df.to_excel(xlsxfn,sheet_name="metadata")

    da=pandas.DataFrame()
    da['Chan name']=__Chan_name
    # check if channels are archived or not
    for idx, row in da.iterrows():
        da.at[idx,"Archived"]=areWeArchiving(da.at[idx,"Chan name"])
  
    with pandas.ExcelWriter(xlsxfn,
                            engine="openpyxl",
                            mode="a") as writer:
        da.to_excel(writer, sheet_name='Archived')
        
    # getDataAtTime: take snap shot of channels at this time point.
    now=datetime.datetime.now().astimezone(JST) # EPICS AA requires TZ info.
    resp=getDataAtTime(
        __Chan_name,
        at=now,
    )
    
    df=pandas.DataFrame(resp).transpose()
    logging.info(df)
    with pandas.ExcelWriter(
            xlsxfn,
            engine="openpyxl",
            mode="a") as writer:
        df.to_excel(writer, sheet_name=f'@ {now.ctime().replace(":","_")}'[:31])

    # wbk=openpyxl.open(xlsxfn)
    # logging.info(wbk.worksheets)
    # for sheet in ("Archived", "snapshot"):
    #     if sheet in wbk:
    #         del wbk[sheet]
    # wbk.save(xlsxfn)
    # wbk.close()

def test_postprocess(chn, nsamples=1200):
    """
    max_th=4 -> 141.113u 4.184s 1:07.69 214.6%	0+0k 0+0io 67pf+0w
    """
    import openpyxl,pandas
    import pandas,numpy
    import math
    import matplotlib.pyplot as pyplot
    
    pps=("","mean","std","loess",) #"kurtosis",
    fig, plts=pyplot.subplots(len(pps),1)
    fig.set_size_inches(9.1,5.12)
    to_=datetime.datetime.now()
    #from_=to_-datetime.timedelta(days=7)
    #from_=datetime.datetime(2024,12,1,0,0,0)
    from_=datetime.datetime(2025, 1, 15,0,0,0)
    for plt,pp in zip(plts,pps):
        meta=getMetadata(chn)
        retv=getData(
            chn, from_=from_, to_=to_,
            pp=f"{pp}_{nsamples}" if pp else ""
        )
        data=[
            d for d in retv["data"]
            if ('val' in d)  and not numpy.isnan(d['val'])
        ]
        date=[ datetime.datetime.fromtimestamp(d["secs"]+d["nanos"]/1000000000) for d in data]
        val=[ d["val"] for d in data]
        plt.plot(
            date,
            val,
            label= f"{pp}_{nsamples}" if pp else "raw",
        )
        pyplot.draw()
        av=numpy.average( val)
        xmin,xmax=date[0],date[-1]
        plt.hlines(av,xmin,xmax, linestyle=":")
        plt.text(
            0.9, av,
            f"{av:5.3f}",
            transform=plt.get_yaxis_transform(),
        )
        if pp != "std" and "HIHI" in meta :
            hihi=float(meta["HIHI"])
            lolo=float(meta["LOLO"])
            high=float(meta["HIGH"])
            low =float(meta["LOW"])
            if (hihi > high):
                plt.hlines(hihi,xmin, xmax,linestyle="--",color="red")
                plt.hlines(high, xmin,xmax,linestyle=":",color='orange')
            if (lolo < low):    
                plt.hlines(lolo,xmin,xmax,linestyle="--",color='red')
                plt.hlines(low ,xmin,xmax,linestyle=":", color='orange')
        plt.legend()
    pyplot.tight_layout()
    fig.suptitle(f"{chn} : {from_.isoformat()[:10]} to {to_.isoformat()[:10]}")
    pyplot.draw()
    fig.savefig(f"FIGS/{chn}_{from_.isoformat()[:10]}_{to_.isoformat()[:10]}.png")
    pyplot.close(fig)
    
def  test_postprocess_pb(chn,nsamples=1200):
    """
    max_th=1 ->  78.417u 1.577s 1:59.85 66.7%	0+0k 0+0io 56pf+0w
    max_th=4 ->  98.021u 2.558s 0:36.34 276.7%	0+0k 0+0io 59pf+0w
    max_th=8 -> 154.400u 4.838s 0:28.50 558.7%	0+0k 0+0io 66pf+0w
    """
    import openpyxl,pandas
    import pandas,numpy
    import math
    import matplotlib.pyplot as pyplot

    pps=("", "mean", "std", "loess",)
    nsamples=1200
    fig, plts=pyplot.subplots(len(pps), 1)
    fig.set_size_inches(9.1,5.12)
    to_=datetime.datetime.now()
    # from_=to_-datetime.timedelta(days=7)
    # from_=datetime.datetime(2024, 12, 1, 0, 0, 0)
    from_=datetime.datetime(2025, 1, 15, 0, 0, 0)
    for plt,pp in zip(plts,pps):
        meta=getMetadata(chn)
        chunks=getData(
            chn, from_=from_, to_=to_,
            pp=f"{pp}_{nsamples}" if pp else "",
            fmt="raw"
        )
        ary=numpy.array([(e["date"],e["data"]["val"]) for e in DataIterator(chunks) if e["data"]['val'] != "NaN"])
        plt.plot(
            ary[:,0],ary[:,1],
            label= f"{pp}_{nsamples}" if pp else "raw",
            )
        av=numpy.average(ary[:,1])
        if numpy.isnan(av):
            continue
        xmin,xmax=ary[0,0],ary[-1,0]
        plt.hlines(av, xmin, xmax, linestyle="-.",color="green")
        plt.text(
            0.9, av,
            f"{av:5.3f}",
            transform=plt.get_yaxis_transform(),
        )
        if pp != "std" and "HIHI" in meta :
            hihi=float(meta["HIHI"])
            lolo=float(meta["LOLO"])
            high=float(meta["HIGH"])
            low =float(meta["LOW"])
            if (hihi > high):
                plt.hlines(hihi,xmin, xmax,linestyle="--",color="red")
                plt.hlines(high, xmin,xmax,linestyle=":",color='orange')
            if (lolo < low):    
                plt.hlines(lolo,xmin,xmax,linestyle="--",color='red')
                plt.hlines(low ,xmin,xmax,linestyle=":", color='orange')
        plt.legend()
    pyplot.tight_layout()
    fig.suptitle(f"{chn} : {from_.isoformat()[:10]} to {to_.isoformat()[:10]}")
    pyplot.draw()
    fig.savefig(f"FIGS_PB/{chn}_{from_.isoformat()[:10]}_{to_.isoformat()[:10]}.png")
    pyplot.close(fig)
    
def test_postprocess_all(nsamples=1200):
    max_th=4
    Chan_name=__ai_chan_names + __li_chan_names
    count=0
    futures=[]
    with concurrent.futures.ProcessPoolExecutor(
            max_workers=min(max_th,len(Chan_name))) as e:
        for chn in Chan_name:
            #futures.insert(0,e.submit(test_postprocess   ,chn,nsamples))
            futures.insert(0,e.submit(test_postprocess_pb,chn,nsamples))
    while True:
        done,not_done=concurrent.futures.wait(futures,timeout=10)
        if not not_done:
            logging.info(f"not_done:{not_done}")
            break
        else:
            count +=1
            logging.warning(f"waiting({count}).")
            continue
    logging.warning(f"Done({count}).")
    
if __name__ == "__main__":
    # # 
    # # ´10.64.31.16'= "util-ioc01.mr.jkcont:5064"
    # #os.environ["EPICS_CA_ADDR_LIST"] = " ".join((os.environ["EPICS_CA_ADDR_LIST"],"10.64.31.16"))
    # os.environ["EPICS_CA_ADDR_LIST"] = " ".join((os.environ["EPICS_CA_ADDR_LIST"],"util-ioc01.mr.jkcont:5064"))
    # sys.path.append("/usr/lib64/python3.{}/site-packages/".format(sys.version_info.minor))
    # #import ca
    logging.getLogger().setLevel(logging.INFO)
    # test()
    test_postprocess_all()

