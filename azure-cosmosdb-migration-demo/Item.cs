﻿using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json;

namespace MigrationDemo
{
    public class tags
    {
        public string colors { get; set; }
        public string material { get; set; }
    }
    public class Item
    {
        public string id { get; set; }
        public string pk { get; set; }
        //public string _partitionKey { get; set; }
        public string product_id { get; set; }
        public string product_name { get; set; }
        public string product_category { get; set; }
        public int product_quantity { get; set; }
        public double product_price { get; set; }
        public List<tags> product_tags { get; set; }
        public string sale_department { get; set; }
        public string user_mail { get; set; }
        public string user_name { get; set; }
        public string user_country { get; set; }
        public string user_ip { get; set; }
        public string user_avatar { get; set; }
        public string user_comments { get; set; }
        public bool user_isvip { get; set; }
        public string user_login_date { get; set; }
        public string timestamp { get; set; } 

        //system properties
        //public string _rid { get; set; }
        //public string _self { get; set; }
        //public string _etag { get; set; }
        //public string _attachments { get; set; }
        //public int _ts { get; set; }
    }
}
