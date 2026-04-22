namespace IoTSharp.Data.Taos.Protocols.TDWebSocket
{
    public class WSConnRsp : WSActionRsp
    {
        public long req_id { get; set; }

        public long timing { get; set; }

        public string version { get; set; }
    }


}