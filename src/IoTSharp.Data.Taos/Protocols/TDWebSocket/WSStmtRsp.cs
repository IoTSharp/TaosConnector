namespace IoTSharp.Data.Taos.Protocols.TDWebSocket
{
    public class WSStmtRsp : WSActionRsp
    {
        public long req_id { get; set; }
        public long timing { get; set; }
        public long stmt_id { get; set; }
    }
    public class WSStmtExecRsp : WSStmtRsp
    {
        public int affected { get; set; }
    }
}