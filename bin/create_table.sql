create table lenovo_user_gain_data_rel (
  lenovoid    string,
  cs_customerid     string,
  microblogid    string,
  wechatid    string,
  mobile_md5    string,
  email    string,
  unique_cookie    string,
  cookie    string,
  imei_num    string,
  create_time    string
)
partitioned by (src string,dtd string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;


create table super_v2 (
  super_id    string,
  relation    string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\005'
STORED AS TEXTFILE;


load data local inpath '/home/appadmin/lzy/hive_100' into table user_relation;

create table user_relation (
  lenovoid    string,
  cs_customerid     string,
  microblogid    string,
  wechatid    string,
  mobile_md5    string,
  email    string,
  unique_cookie    string,
  imei_num    string,
  lps_did    string,
  create_time    string,
  src    string,
  dtd    string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE;