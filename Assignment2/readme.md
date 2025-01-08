names, ids:
yaniv efremov , 324265016 
guy meir , 211789862

the result of a run on the full corpus:
https://wordcounttets.s3.us-east-1.amazonaws.com/full_courpus/output_step3/part-r-00000

steps to run the project: (in case the precompiled file are used skip steps 2 , 3 and set bucketName to wordcounttets)
1.enter the probCheck folder
cd probCheck

2.compile and upload the jars by changing the pom file main class using 
mvn clean package (the step will need to be changed in the pom build section)
(in case you dont you use our precompiled jars.)

3.upload to your wanted bucket
aws s3 cp target/LocalApp-1.0-SNAPSHOT.jar s3://{bucketName}/jars/step{i}.jar

4.configure the wanted amount of instances in app.java default being 1 and compile after the change
mvn clean package (note no change to the pom file is needed)

5.run the command mvn exec:java -Dexec.mainClass=App -Dexec.args = "{bucketName}"

running with different ammounts of mappers:
1 full corpus - 55 minutes
7 full corpus - 15 minutes
1 one split - 8.5 minutes
7 one split - 8 minutes


the number of key value pairs:
without local aggregation 4264491 (step 1)  + 197310 (step 2) + 34228 (step 3) = 4496029
with local aggregartion  4264491 (step 1) + 197310 (step 2 ) + 34228 (step 3) = 4496029


    לאחר תקופה קצרה 0.3264176728985014
	לאחר תקופה ארוכה	0.2768453088071887
	לאחר תקופה ממושכת	0.10679631049035965
	לאחר תקופה מסוימת	0.06204166595068254
	לאחר תקופה מסויימת	0.038031481672742576

    we believe the system got a reasonable decision since it makes sense for an adjective that describes time to come after לאחר תקופה

    עוד בימי קדם	0.07310672720480385
	עוד בימי נעוריו	0.06236635944643012
	עוד בימי חייו	0.05890649280735873
	עוד בימי בית	0.04207097728812235
	עוד בימי מלחמת	0.038566214531800064

    we think the correct decision was made here since first we have adjectives which could be more generala nd than we have 
    3 word phrases which are quite commonly used

    עוד בשנת 1933	0.016869706223582307
	עוד בשנת 1940	0.016833767612834956
	עוד בשנת 1939	0.01666984686918954
	עוד בשנת 1925	0.016266729432033854
    עוד בשנת 1919	0.015101511432480928

    we believe this is a logical outcome since the years with the highest likelyhood are all around the world wars those being the most influential years to 
    our modern history however since a year is something that has a lot of possible loggicl options the odds are still slim

    רבי אברהם בן	0.058410159594559835
	רבי אברהם יצחק	0.050205894112396725
	רבי אברהם יהושע	0.04552058139807668
	רבי אברהם אבן	0.044166427479478486
	רבי אברהם יעקב	0.035544699755332275

    this seem logical since rabbies often have "double names" so the name rabbie abrahm and then a second name in not out of the ordinary

    הנער בן מאה	0.5340820109434895
	הנער בן השש	0.07956393332705958
	הנער בן הארבע	0.06454886691534178
	הנער בן השבע	0.06320202288897625
	הנער בן השלוש	0.05911957596502654

    at first we thought this may be a mistake but upon further reaserch the 100 year old boy is part of a pasuk so it makes since for it 
    to be more likely then just a random age

    הסופרים העברים בישראל	0.10519352179899677
	הסופרים העברים בארץ	0.06331451556759256
	הסופרים העברים ליד	0.05746235195899045
	הסופרים העברים במדינת	0.02830606561010386
	הסופרים העברים באמריקה	0.020541874541724542

    we think this is a reasonable decision since it shows that when talking about authors in hebrew we see tht first the topic is one in israel
    which has the largest population of hebrew speaker and than the words become broader until getting to the suncd largest jewish population which is in america

    מבחינה מדינית וכלכלית	0.14293477900543403
	מבחינה מדינית וצבאית	0.08466847022910183
	מבחינה מדינית והן	0.06777980240476698
	מבחינה מדינית וחברתית	0.05936748266155633
	מבחינה מדינית ותרבותית	0.04399484418499222

    we think this is a correct decision since when saying the words מבחינה מדינית you would expect the next word to be related to the reality of our countries policies
    which are generaly aabout economics and military but do also include social and culture related policies

    מבחינה פוליטית ותרבותית	0.040804509262622746
	מבחינה פוליטית וצבאית	0.03130061938786107
	מבחינה פוליטית וגם	0.028974699209887874
	מבחינה פוליטית ומוסרית	0.026563864830335383
	מבחינה פוליטית ומבחינה	0.023609554851293048

    again foloowing the trend from the previous five the topics are related to our countries politics but the third word now goes to a more 
    human prespective rather than purely about policies


    מחוץ לגבולות המדינה	0.1927272187692678
	מחוץ לגבולות הארץ	0.14183033032616071
	מחוץ לגבולות ארץ	0.10118180759045155
	מחוץ לגבולות רוסיה	0.04756017734884158
	מחוץ לגבולות גרמניה	0.041423266772755866

    we think this is also a logical outcome since first you have a general state and than the next most likely are specific countries 
    and even more than that countries which relate heavily to hebrew speakers

    לנקוט באמצעים הדרושים	0.19068378557982663
	לנקוט באמצעים כדי	0.17072540080314527
	לנקוט באמצעים נגד	0.15430414946546828
	לנקוט באמצעים חריפים	0.14067245788879343
	לנקוט באמצעים חמורים	0.09997946635365854

    we think this outcome is correct since this are the most commonley used words after לנקוט באמצעים words that are more often than not
    firm and "a call to action" a gaint something