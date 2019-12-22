create   table t_order
(oid int ,
 uid int ,
 otime date,
 oamount int
)partitioned by (dt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';


insert into table t_order  partition(dt ='2018-01-01') values(1003,2,'2018-01-01',100);
insert into table t_order  partition(dt ='2018-01-02') values(1004,2,'2018-01-02',20);
insert into table t_order  partition(dt ='2018-01-02') values(1005,2,'2018-01-02',100);
insert into table t_order  partition(dt ='2018-01-02') values(1006,4,'2018-01-02',30);
insert into table t_order  partition(dt ='2018-01-03') values(1007,1,'2018-01-03',130);
insert into table t_order  partition(dt ='2018-01-03') values(1008,2,'2018-01-03',5);
insert into table t_order  partition(dt ='2018-01-03') values(1009,2,'2018-01-03',5);

insert into table t_order  partition(dt ='2018-02-01') values(1001,5,'2018-02-01',110);
insert into table t_order  partition(dt ='2018-02-01') values(1002,3,'2018-02-01',110);
insert into table t_order  partition(dt ='2018-02-03') values(1003,3,'2018-02-03',100);
insert into table t_order  partition(dt ='2018-02-03') values(1004,3,'2018-02-03',20);
insert into table t_order  partition(dt ='2018-02-04') values(1005,3,'2018-02-04',30);
insert into table t_order  partition(dt ='2018-02-04') values(1006,6,'2018-02-04',100);
insert into table t_order  partition(dt ='2018-02-04') values(1007,6,'2018-02-04',130);

insert into table t_order  partition(dt ='2018-03-01') values(1001,1,'2018-03-01',120);
insert into table t_order  partition(dt ='2018-03-03') values(1002,2,'2018-03-03',5);
insert into table t_order  partition(dt ='2018-03-03') values(1003,2,'2018-03-03',11);
insert into table t_order  partition(dt ='2018-03-03') values(1004,3,'2018-03-03',1);
insert into table t_order  partition(dt ='2018-03-04') values(1005,3,'2018-03-04',20);
insert into table t_order  partition(dt ='2018-03-04') values(1006,4,'2018-03-04',30);
insert into table t_order  partition(dt ='2018-03-04') values(1007,1,'2018-03-04',50);

select
    uid,
    sum(if(substr(otime,0,7)='2018-03' and rn2 = 1,oamount,0)) as month3_first_order,
    sum(if(substr(otime,0,7)='2018-03' and rn1 = 1,oamount,0)) as month3_last_order,
    sum(if(substr(otime,0,7)='2018-01',1,0)) as month1_cnt,
    sum(if(substr(otime,0,7)='2018-02',1,0)) as month2_cnt,
    sum(if(substr(otime,0,7)='2018-03',1,0)) as month3_cnt
from
    (select
        *,
        row_number() over(partition by uid,substr(otime,0,7) order by otime desc) as rn1,
        row_number() over(partition by uid,substr(otime,0,7) order by otime asc) as rn2
    from t_order
    where oamount>10) a
group by uid
having month1_cnt >  0 and month2_cnt = 0;