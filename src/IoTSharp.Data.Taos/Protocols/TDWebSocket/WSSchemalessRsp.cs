namespace IoTSharp.Data.Taos.Protocols.TDWebSocket
{
    public class WSSchemalessRsp: WSActionRsp
    {
        public long req_id { get; set; }
        public long timing { get; set; }

    }
}