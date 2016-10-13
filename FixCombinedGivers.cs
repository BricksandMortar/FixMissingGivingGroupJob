using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Quartz;
using Rock;
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
            var adultRoleGuid = Rock.SystemGuid.GroupRole.GROUPROLE_FAMILY_MEMBER_ADULT.AsGuid();

            //Create KeyPairValues where the FamilyId is the key and the PersonId is the value
            var familys = new GroupService( rockContext )
                .Queryable( "Members,Members.Person" )
                .Where( g => g.GroupTypeId == familyGroupType.Id && g.Members.Any( m => m.Person.GivingGroupId == 0 || m.Person.GivingGroupId == null ) )
                .SelectMany( g => g.Members.Select( p => new KeyValuePair<int, int>( g.Id, p.Id ) ) );

            int count = 0;
            var personService = new PersonService( rockContext );
            foreach ( var family in familys )
            {
                var person = personService.Get( family.Value );

                //Only set people who are missing a GivingGroup
                if ( person.GivingGroupId != null && person.GivingGroupId != 0 )
                {
                    continue;
                }

                //Set the GivingGroup as the FamilyId
                person.GivingGroupId = family.Key;
                count++;
                if ( count > 100 )
                {
                    rockContext.SaveChanges();
                    //Replace the context and service as the objects become bloated
                    rockContext = new RockContext();
                    personService = new PersonService( rockContext );
                    count = 0;
                }
            }
            rockContext.SaveChanges();
        }

    }
}
