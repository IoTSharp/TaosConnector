using System;
using System.Runtime.InteropServices;
using System.Text;


namespace TDengineDriver
{
    public struct UTF8PtrStruct
    {
        public IntPtr utf8Ptr { get; set; }
        public int utf8StrLength { get; set; }

        public UTF8PtrStruct(string str)
        {
            utf8StrLength = Encoding.UTF8.GetByteCount(str);
            utf8Ptr = Marshal.StringToCoTaskMemUTF8(str);
        }
        public void UTF8FreePtr()
        {
            Marshal.FreeCoTaskMem(utf8Ptr);
        }

    }
}