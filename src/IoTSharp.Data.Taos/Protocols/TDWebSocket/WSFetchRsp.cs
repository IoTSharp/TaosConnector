using System.Collections.Generic;

namespace IoTSharp.Data.Taos.Protocols.TDWebSocket
{
    public class WSFetchRsp : WSActionRsp
    {
        public long req_id { get; set; }

        public long timing { get; set; }

        public long id { get; set; }

        public bool completed { get; set; }

        public List<int> lengths { get; set; }

        public int rows { get; set; }
    }


}