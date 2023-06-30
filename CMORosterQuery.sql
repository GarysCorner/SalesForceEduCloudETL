
--alter view [dbo].[view_SalesForceEduCloud_CMORoster] as


select --top 100
	acc.Id as [StudentSalesForceId],
	c.Id as [ContactSalesForceId],
	a.Id as [EnrollmentSalesForceId],
	a.[Name],
	a.[Active__c],
	a.[CreatedDate],
	a.Grade__c,
	acc.FirstName,
	acc.LastName,
	a.[School_Name__c],
	a.[School_Site_Code__c],
	acc.Phone,
	c.Email,
	a.Student_Birthdate__c,
	c.GenderIdentity,
	a.Current_T9__c,
	a.[EnrollmentDate],
	c.MailingStreet,
	c.MailingCity,
	c.MailingPostalCode




 from SalesForceEduCloud_AcademicTermEnrollment as a
 left join SalesForceEduCloud_Account as acc
	on acc.Id = a.LearnerAccountId

left join SalesForceEduCloud_Contact as c
	on c.id = a.LearnerContactId
 
 ;


