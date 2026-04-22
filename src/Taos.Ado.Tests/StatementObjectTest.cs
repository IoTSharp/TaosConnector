using IoTSharp.Data.Taos;
using System;
using System.Linq;
using Xunit;

namespace Taos.Ado.Tests
{
    public class StatementObjectTest
    {
        [Fact]
        public void TestExecuteBulkInsert_Json()
        {
            const string SQLDemo1 = "";
            const string SQLDemo2 = @"SELECT DISTINCT tbname,point_code,pol_code,station_code,station_name/* 站点名 */
    ,station_uid,longitude,latitude,mn,dev_code,pol_name,pol_unit,pol_id FROM st_pmcm WHERE point_code = @p1;";
            const string SQLDemo3 = @"SELECT * FROM bytable WHERE t1 = @t1 AND t2 LIKE @t11 LIMIT @t3;";
            const string SQLDemo4 = @"INSERT INTO t_status USING st_status (status_key) TAGS ('s1') (status_time, val) VALUES (@x1, @x2);";
            var s1 = StatementObject.ResolveCommandText(SQLDemo1);
            Assert.Empty(s1);
            Assert.Equal(string.Empty, s1.CommandText);
            var s2 = StatementObject.ResolveCommandText(SQLDemo2);
            Assert.Equal(5, s2.Count);
            Assert.Equal("SELECT DISTINCT tbname,point_code,pol_code,station_code,station_name", s2[0].OriginalText);
            Assert.Equal("/* 站点名 */", s2[1].OriginalText);
            Assert.Equal(@"
    ,station_uid,longitude,latitude,mn,dev_code,pol_name,pol_unit,pol_id FROM st_pmcm WHERE point_code = ", s2[2].OriginalText);
            Assert.Equal("@p1", s2[3].OriginalText);
            Assert.Equal(";", s2[4].OriginalText);
            Assert.Equal(@"SELECT DISTINCT tbname,point_code,pol_code,station_code,station_name 
    ,station_uid,longitude,latitude,mn,dev_code,pol_name,pol_unit,pol_id FROM st_pmcm WHERE point_code = ?;", s2.CommandText);
            var pn2 = s2.ParameterNames;
            Assert.Single(pn2);
            Assert.Equal("@p1", pn2[0]);
            var s3 = StatementObject.ResolveCommandText(SQLDemo3);
            Assert.Equal(7, s3.Count);
            Assert.Equal("SELECT * FROM bytable WHERE t1 = ", s3[0].OriginalText);
            Assert.Equal("@t1", s3[1].OriginalText);
            Assert.Equal(" AND t2 LIKE ", s3[2].OriginalText);
            Assert.Equal("@t11", s3[3].OriginalText);
            Assert.Equal(" LIMIT ", s3[4].OriginalText);
            Assert.Equal("@t3", s3[5].OriginalText);
            Assert.Equal(";", s3[6].OriginalText);
            Assert.Equal("SELECT * FROM bytable WHERE t1 = ? AND t2 LIKE ? LIMIT ?;", s3.CommandText);
            var pn3 = s3.ParameterNames;
            Assert.Equal(3, pn3.Count());
            Assert.Equal("@t1", pn3[0]);
            Assert.Equal("@t11", pn3[1]);
            Assert.Equal("@t3", pn3[2]);
            var s4 = StatementObject.ResolveCommandText(SQLDemo4);
            Assert.Equal(7, s4.Count);
            Assert.Equal("INSERT INTO t_status USING st_status (status_key) TAGS (", s4[0].OriginalText);
            Assert.Equal("'s1'", s4[1].OriginalText);
            Assert.Equal(") (status_time, val) VALUES (", s4[2].OriginalText);
            Assert.Equal("@x1", s4[3].OriginalText);
            Assert.Equal(", ", s4[4].OriginalText);
            Assert.Equal("@x2", s4[5].OriginalText);
            Assert.Equal(");", s4[6].OriginalText);
            Assert.Equal("INSERT INTO t_status USING st_status (status_key) TAGS ('s1') (status_time, val) VALUES (?, ?);", s4.CommandText);
            var pn4 = s4.ParameterNames;
            Assert.Equal(2, pn4.Count());
            Assert.Equal("@x1", pn4[0]);
            Assert.Equal("@x2", pn4[1]);
            var s5 = StatementObject.ResolveCommandText("insert into #subtable using stable tags($t1,$t2,$t3,$t4,$t5,$t6,$t7,$t8,$t9,$t10,$t11,$t12,$t13) values (@t1,@t2,@t3,@t4,@t5,@t6,@t7,@t8,@t9,@t10,@t11,@t12,@t13,@t14)");
            Assert.Equal(14, s5.ParameterNames?.Count());
            Assert.Equal("#subtable", s5.SubTableName);
            Assert.Equal(13, s5.TagsNames.Count());
            Assert.Equal("insert into ? using stable tags(?,?,?,?,?,?,?,?,?,?,?,?,?) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", s5.CommandText);
        }
    }
}
