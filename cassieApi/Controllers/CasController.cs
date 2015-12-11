using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Web;
using System.Web.Http;
using cassieApi.Models;
using Cassandra;
using Cassandra.Mapping;

namespace cassieApi.Controllers
{
    public class CasController : ApiController
    {
        private static string Users_Table = System.Configuration.ConfigurationManager.AppSettings["UserTable"];
        private static IMapper _mapper;

        static CasController()
        {
            var cluster = Cluster.Builder()
              .AddContactPoints(System.Configuration.ConfigurationManager.
                    ConnectionStrings["Cassandra"].ConnectionString.Split(',').Select(x => x.Trim()))
              .Build();

            //Create connections to the nodes using a keyspace
            var session = cluster.Connect(System.Configuration.ConfigurationManager.AppSettings["UserKeyspace"]);
            MappingConfiguration.Global.Define(
               new Map<User>()
                  .TableName(Users_Table)
                  .PartitionKey(u => u.Username)
                  .Column(u => u.Username, cm => cm.WithName("username"))
                  .Column(u => u.Id, cm => cm.WithName("id")));



            _mapper = new Mapper(session);

        }


        public IHttpActionResult Post([FromBody] User user)
        {
            string uri = string.Format(CultureInfo.CurrentCulture, "{0}/{1}", Request.RequestUri, user.Username);
            User nUser = new User {Id = Guid.NewGuid(), Username = user.Username};
            _mapper.Insert(nUser);
            
            return Created(uri, nUser);
        }

        public IHttpActionResult Get(string username)
        {
            IEnumerable<User> users = _mapper.Fetch<User>($"SELECT id, username FROM {Users_Table}");
            return Ok(users.FirstOrDefault(x => x.Username == username));
        }

    }
}