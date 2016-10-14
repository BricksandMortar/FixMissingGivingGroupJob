using System.Linq;
using Quartz;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;

namespace com.bricksandmortarstudio.FixCombinedGivers
{
    [DisallowConcurrentExecution]
    public class FixCombinedGivers : IJob
    {
        public void Execute( IJobExecutionContext context )
        {
            var rockContext = new RockContext();

            var familyGroupType = GroupTypeCache.Read( Rock.SystemGuid.GroupType.GROUPTYPE_FAMILY );
            var familyMembers = new GroupMemberService( rockContext )
              .Queryable( "Group,Person" )
              .Where(
                  g =>
                      g.Group.GroupType.Id == familyGroupType.Id && g.Person.GivingGroupId == 0 ||
                      g.Person.GivingGroupId == null );
            while (familyMembers.Any())
            {
                //Chunk to prevent foreach saving thread errors
                foreach ( var chunk in familyMembers.OrderBy( f => f.Id ).QueryChunksOfSize( 100 ) )
                {
                    foreach ( var familyMember in chunk )
                    {
                        familyMember.Person.GivingGroupId = familyMember.GroupId;
                    }
                    rockContext.SaveChanges();
                }
                rockContext = new RockContext();
                familyMembers = new GroupMemberService( rockContext )
              .Queryable( "Group,Person" )
              .Where(
                  g =>
                      g.Group.GroupType.Id == familyGroupType.Id && g.Person.GivingGroupId == 0 ||
                      g.Person.GivingGroupId == null );
            }
           
        }

    }
}
