import datetime as dtt
from datetime import date, timedelta,datetime
import pyodbc
import csv
import socket
import urllib.request


print(dtt.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]+", "+"Script Started\n")
URL = "https://hc-ping.com/7ae0e8e6-6a51-459d-bdd2-b336b0f0a966"
pyodbc.autocommit = True
pyodbc.pooling = False


#2 hari kebelakang
Previous_Date = date.today() - timedelta(days=2)
#given_date = datetime.today().date()
 
First_day_of_month = Previous_Date - timedelta(days = int(Previous_Date.strftime("%d"))-1)


Last_day_of_prev_month = Previous_Date.replace(day=1) - timedelta(days=1)

Start_day_of_prev_month = Previous_Date.replace(day=1) - timedelta(days=Last_day_of_prev_month.day)


cnxn = pyodbc.connect("DSN=hive64", autocommit=True)
print("Connected\n")

try:
    requests.get(URL + "/start", timeout=10)
except requests.exceptions.RequestException:
    # If the network request fails for any reason, we don't want
    # it to prevent the main job from running
    pass

#cur_time = dtt.datetime.now().strftime('%Y-%m-%d_%H_%M_%S_%f')[:-3]

file_output = 'pack_voice_mbjj_mtd'+"_"+Previous_Date+".txt"

# connect
cursor = cnxn.cursor()

sqlquery ="""
SELECT '"""+str(Previous_Date)+"""', z.region_sales, z.cluster_sales, z.kabupaten, b.site_id, g.new_taker, g.ICR, 
CASE WHEN m.msisdn IS NOT NULL THEN 'Yes' ELSE 'No' END AS mbjj_wl,
COUNT(distinct c.msisdn), sum(c.trx), sum(c.rev), sum(g.rev_m1), sum(g.rev_mtd)

FROM 
(Select msisdn, cgi_dom_mtd, region_sales, cluster_sales, kabupaten
FROM Hive.ar_v.v_cb_multidim
where region_sales = 'SUMBAGTENG'
AND event_date = '"""+str(Previous_Date)+"""') z

INNER JOIN
(SELECT msisdn, sum(trx) as trx, sum(rev) as rev 
FROM Hive.rna_all_v.v_merge_revenue_dd
WHERE  l1_name = 'Voice P2P' and l2_name = 'Voice Package'
and (event_date BETWEEN '"""+str(First_day_of_month)+"""' AND '"""+str(Previous_Date)+"""') 
group by 1) c
on z.msisdn = c.msisdn

LEFT JOIN 
(SELECT f.msisdn, case when f.rev_mtd > e.rev_m1 then 'inc' else 'not inc' end as ICR,
case when e.msisdn is NOT NULL then 'NO' else 'YES' end as new_taker, e.rev_m1, f.rev_mtd

FROM
(SELECT msisdn, sum(rev) as rev_mtd
FROM Hive.rna_all_v.v_merge_revenue_dd
WHERE  l1_name = 'Voice P2P' and l2_name = 'Voice Package'
and (event_date BETWEEN '"""+str(First_day_of_month)+"""' AND '"""+str(Previous_Date)+"""') 
group by 1) f

LEFT JOIN
(SELECT msisdn, sum(rev) as rev_m1 
FROM Hive.rna_all_v.v_merge_revenue_dd
WHERE  l1_name = 'Voice P2P' and l2_name = 'Voice Package'
and (event_date BETWEEN '"""+str(Start_day_of_prev_month)+"""' AND '"""+str(Last_day_of_prev_month)+"""') 
group by 1) e
on f.msisdn = e.msisdn) g
on c.msisdn = g.msisdn

LEFT JOIN
(SELECT msisdn, region, COUNT(DISTINCT(msisdn)) as rnk
FROM rna_all_v.baseline_mbjj_batch10 WHERE region IS NOT NULL
GROUP BY 1,2) m
ON z.msisdn = m.msisdn

LEFT JOIN
(select lacci, site_id, count(site_name)
    from ar_v.laccima_dim
    where (event_date = '"""+str(Previous_Date)+"""')
    group by 1,2) b 
on z.cgi_dom_mtd=b.lacci 

group by 1,2,3,4,5,6,7,8;
"""
    


print(dtt.datetime.now().strftime("%A %Y-%m-%d %H:%M:%S") + ", " + "executing query --> " + sqlquery + "\n")
number_of_rows = cursor.execute(sqlquery)
print(dtt.datetime.now().strftime("%A %Y-%m-%d %H:%M:%S") + ", " + "writing output --> " + file_output + "\n")

fib = lambda n: n if n < 2 else fib(n - 1) + fib(n - 2)
print("F(42) = %d" % fib(42))

try:
    urllib.request.urlopen("https://hc-ping.com/7ae0e8e6-6a51-459d-bdd2-b336b0f0a966", timeout=10)
except socket.error as e:
    # Log ping failure here...
    print("Ping failed: %s" % e)

requests.get(URL)

delim = "|"
quote = '"'
f = open(file_output, 'w')
seq = 0
seqerr = 0

header = ""
for column in cursor.description:
    header += quote + str(column[0]) + quote + delim
f.write(header + "\n")

while True:
    seq = seq + 1

    try:
        row = cursor.fetchone()
        # row = row.encode('utf8').strip()
        if row == None:
            break
        # if seq == 2:
        #        break
        line = ""
        for rec in row:
            if str(type(rec)) == "<type 'unicode'>":
                rec = rec.encode('utf8').strip()

            line += quote + str(rec) + quote + delim

            # line += quote+str(rec.decode('utf-8','ignore').encode("utf-8"))+quote + delim
        # print line
        f.write(line + "\n")
    except Exception as e:
        seqerr = seqerr + 1
        print(type(e))
        print(e.args)
        print("skipping line "+str(seq)+"\n")
        # f.write("skipping line "+str(seq)+"\n")
        # f.write(type(e))
        # f.write(e.args)
        pass
cnxn.commit()
cursor.close

cnxn.close

print("\n"+dtt.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-4]+", "+"Succesfully Disconnect\n")
