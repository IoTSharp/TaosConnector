using System.Collections.Generic;

namespace IoTSharp.Data.Taos.Protocols.TDWebSocket
{
    public class WSQueryRsp : WSActionRsp
    {

        public long req_id { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public long timing { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public long id { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public bool is_update { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public int affected_rows { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public int fields_count { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public List<string> fields_names { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public List<byte> fields_types { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public List<long> fields_lengths { get; set; }
        public int precision { get; set; }
    }


}