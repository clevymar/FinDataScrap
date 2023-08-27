import yagmail
import pandas as pd


SENDER_EMAIL = "cyrilpython@gmail.com"  # Enter your address
PASSWORD = "lixfpzjxqyvynorh"
RECEIVER_EMAIL = "cyrilroutines@gmail.com"  # Enter receiver address
READWISE_EMAIL="rthkpzoi@library.readwise.io"


def nice_pivot(pt,title="",color_border="green",min_width=100,min_chars=6,digits=0):
    if title =="":
        header=""
    else:
        header=f'<h3>{title}</h3>'
    format_str="{:,."+str(digits)+"f}"

    header+=f'<table border="1" style="border: 5px solid {color_border};border-collapse: collapse">'
    header+=f'<thead style="background-color:lightblue">'

    cols=pt.reset_index().columns
    align='right'
    for idx,col in enumerate(cols):
        if len(col)<min_chars:
            col='&nbsp;'*(min_chars-len(col)) + col
        if idx==0: 
            align='left'
        else:
            align='right'
        header+=f'<th style="text-align: {align};min-width: {min_width}px;padding:3px">{col}</th>'
    header+=f'</tr></thead>'

    body='<tbody>'
    pivotHeads=pt.index.get_level_values(0).unique()
    if pt.shape[1]==1:
        dtype="Serie"
    else:
        dtype="DataFrame"

    for idx in pivotHeads:
        subData=pt.loc[idx]
        #print(pt)
        #print(subData)
        body+=f'<tr style="text-align: right; border-top: 2px solid {color_border}">'
        body+=f'<th  style="text-align: left;padding:3px" rowspan={len(subData)}>{idx}</th>'
        for item,row in subData.iterrows():
            body+=f'<td style="text-align: right;padding:3px"> {item}</td>'
            if dtype=="Serie":
                x=format_str.format(row[0])
                body+=f'<td style="text-align: right;padding:3px"> {x}</td>'
            elif isinstance(row, pd.DataFrame):
                for col in row.columns:
                    x=row.loc[col]
                    if isinstance(x,float): x=format_str.format(x)
                    body+=f'<td style="text-align: right;padding:3px"> {x}</td>'
            else:  #serie
                for nb in row:
                    x=nb
                    if isinstance(x,float): x=format_str.format(x)
                    body+=f'<td style="text-align: right;padding:3px"> {x}</td>'
            body+='</tr>'

    body+='</tbody></table>'
    res=header+body
    return res


def nice_table(df,title="",color_border="green",min_width=100,min_chars=6,digits=1,comma=False):
    df2=df.reset_index()
    if title =="":
        header=""
    else:
        header=f'<h3>{title}</h3>'
    format_str="{:."+str(digits)+"f}"
    if comma :format_str="{:,."+str(digits)+"f}"
    header+=f'<table border="1" style="border: 5px solid {color_border};border-collapse: collapse">'
    header+=f'<thead style="background-color:lightgrey">'
    #header+=f'<tr style="text-align: right">'
    col=df2.columns[0]
    if len(col)<min_chars:
            col= col + '&nbsp;'*(min_chars-len(col)) 
    header+=f'<th style="text-align:left;min-width: {min_width}px; padding:3px">{col}</th>'
    for col in df2.columns[1:]:
        if len(col)<min_chars:
            col='&nbsp;'*(min_chars-len(col)) + col
        header+=f'<th style="text-align: right;min-width: {min_width}px;padding:3px">{col}</th>'
    header+=f'</tr></thead>'

    body='<tbody>'
    for index,row in df2.iterrows():
        body+=f'<tr style="text-align: right; border-bottom: 2px dotted {color_border}">'
        body+=f'<th style="text-align: left;padding:3px"> {row.iloc[0]}</th>'
        for col in df2.columns[1:]:
            x=row.loc[col]
            if col!="Last":
                if isinstance(x,float):x=format_str.format(x)
            body+=f'<td style="padding:3px"> {x}</td>'
        body+='</tr>'
    body+='</tbody></table>'

    res=header+body
    return res

#yagmail.register(sender_email,password)


def send_email(subject,body,attachment="", to_reader=False):
    yag = yagmail.SMTP(SENDER_EMAIL,PASSWORD)
    if to_reader: 
        dest=READWISE_EMAIL
    else:
        dest=RECEIVER_EMAIL
    
    if attachment=="":
        yag.send(to=dest,subject=subject,contents=body)
    else:
        yag.send(
                to=dest,
                subject=subject,
                contents=body, 
                attachments=attachment
            )


def send_cyril_andrea(subject,body,attachment=""):
    yag = yagmail.SMTP(SENDER_EMAIL,PASSWORD)
    if attachment=="":
        yag.send(to=[RECEIVER_EMAIL,"andreabaumeister@me.com"],subject=subject,contents=body)
    else:
        yag.send(
                to=[RECEIVER_EMAIL,"andreabaumeister@me.com"],
                subject=subject,
                contents=body, 
                attachments=attachment
        )


def send_andrea(subject,body,attachment=""):
    yag = yagmail.SMTP(SENDER_EMAIL,PASSWORD)
    if attachment=="":
        yag.send(to=RECEIVER_EMAIL,subject=subject,contents=body)
    else:
        yag.send(
                to="andreabaumeister@me.com",
                subject=subject,
                contents=body, 
                attachments=attachment
        )