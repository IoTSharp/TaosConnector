using IoTSharp.Data.Taos;
using System;
using Xunit;

namespace Taos.Ado.Tests
{
    public class TestRemove
    {
        [Theory]
        [InlineData("test\0test", "test")]
        [InlineData("\0test", "")]
        [InlineData("test\0", "test")]
        [InlineData("", "")]
        [InlineData("\0\0", "")]
        [InlineData(null, null)]
        public void TestRemoveNull(string src, string exp)
        {
            var d = src.RemoveNull();
            Assert.Equal(exp, d);
        }
    }
}
