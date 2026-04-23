using Carbunql.Analysis;
using Xunit;

namespace Taos.Ado.Tests
{
    public class SqModelTest
    {
        [Fact]
        public void TestSelectQueryParser_WithParameters()
        {
            const string SQLDemo3 = "SELECT * FROM bytable WHERE t1 = @t1 AND t2 LIKE @t11 LIMIT @t3;";
            var s3 = SelectQueryParser.Parse(SQLDemo3);
            Assert.NotNull(s3);
        }
    }
}
