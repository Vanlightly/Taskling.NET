DECLARE @Counter int = 1
DECLARE @Current DateTime = (SELECT COALESCE(MAX(TravelDate), 0) FROM [MyAppDb].[dbo].[Journey])

WHILE @Current <= GETDATE()
BEGIN
	INSERT INTO [MyAppDb].[dbo].[Journey]
           ([DepartureStation]
           ,[ArrivalStation]
           ,[TravelDate]
           ,[PassengerName])
     VALUES
           ('DEP' + CAST(@Counter AS VARCHAR(5))
           ,'ARR' + CAST(@Counter AS VARCHAR(5))
           ,@Current
           ,'Passenger' + CAST(@Counter AS VARCHAR(5)))
	

	SET @Current = DATEADD(SECOND, 1, @Current)
	SET @Counter = @Counter + 1
END