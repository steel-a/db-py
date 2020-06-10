# dbpy
My database connection package

To run the automatic tests, it's necessary:
- a mysql database.
- a file named keys_test.ini with the connection string, like that:
```
connectionStringTest = user='root', password='123456',host='172.17.0.2', port='3306',database='test'
```
Note: For normal use, you can name the 'ini' file as you want, or use no file, passing the connectionString directly.

I use the following directory structure:
~/apps/packages/dbpy/
~/apps/myApp/
~/apps/keys_test.ini

If you use the same directory structure as me, you can use ~/apps/packages/dbpy/requirements/install-requirements.sh to instal all necessary requirements.