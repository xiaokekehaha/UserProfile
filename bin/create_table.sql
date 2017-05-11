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
partitioned by (dtd string,src string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;


create table super_v1 (
  super_id    string,
  relation    string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\005'
STORED AS TEXTFILE;