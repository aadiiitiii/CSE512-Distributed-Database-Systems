#
# Assignment2 Interface
#

import psycopg2
import os
import sys
from threading import Thread

# Do not close the connection inside this file i.e. do not perform openConnection.close()

def reactangleFragmentation(rectsTable):
    rectangle_partition = []
    rectangle_partition.append(' from {} where (latitude1 <= 40.720758 and longitude1 >= -74.53338) or (latitude2 <= 40.720758 and longitude1 >= -74.53338) or (latitude1 <= 40.720758 and longitude2 >= -74.53338) or (latitude2 <= 40.720758 and longitude2 >= -74.53338)'.format(rectsTable))
    rectangle_partition.append(' from {} where (latitude1 > 40.720758 and longitude1 >= -74.53338) or (latitude2 > 40.720758 and longitude1 >= -74.53338) or (latitude1 > 40.720758 and longitude2 >= -74.53338) or (latitude2 > 40.720758 and longitude2 >= -74.53338)'.format(rectsTable))
    rectangle_partition.append(' from {} where (latitude1 <= 40.720758 and longitude1 < -74.53338) or (latitude2 <= 40.720758 and longitude1 < -74.53338) or (latitude1 <= 40.720758 and longitude2 < -74.53338) or (latitude2 <= 40.720758 and longitude2 < -74.53338)'.format(rectsTable))
    rectangle_partition.append(' from {} where (latitude1 > 40.720758 and longitude1 < -74.53338) or (latitude2 > 40.720758 and longitude1 < -74.53338) or (latitude1 > 40.720758 and longitude2 < -74.53338) or (latitude2 > 40.720758 and longitude2 < -74.53338)'.format(rectsTable))
    return rectangle_partition

def fragmentTables(table_name, fragments, cur):
    resultTables = []
    for fragment_number in range(len(fragments)):
        fragment_table_name = table_name + str(fragment_number+1)
        cur.execute('DROP TABLE IF EXISTS ' + fragment_table_name)
        cur.execute('select * into ' + fragment_table_name + fragments[fragment_number])
        resultTables.append(fragment_table_name)
    return resultTables

def pointFragmentation(pointsTable):
    point_partition = []
    point_partition.append(' from {} where latitude <= 40.720758 and longitude >= -74.53338'.format(pointsTable))
    point_partition.append(' from {} where latitude > 40.720758 and longitude >= -74.53338'.format(pointsTable))
    point_partition.append(' from {} where latitude <= 40.720758 and longitude < -74.53338'.format(pointsTable))
    point_partition.append(' from {} where latitude > 40.720758 and longitude < -74.53338'.format(pointsTable))
    return point_partition

def spatialJoin(point_fragment_table, rectangle_fragment_table, openConnection, parallel_joins, fragment_number):
    cur = openConnection.cursor()
    cur.execute('select r.geom, count(*) from {} r join {} p on ST_Contains(r.geom,p.geom) group by r.geom'.format(rectangle_fragment_table,point_fragment_table))
    
    # This will create a record of all fetched fragments
    joinRecords = cur.fetchall()
    cur.close()
    joinRecords.sort(key = lambda x: x[1])
    parallel_joins[fragment_number] = joinRecords
    
def mergeParallelJoin(parallelJoins):
    parallelJoin = []
    countMap = {}
    for record in parallelJoins:
        for index, count in record:
            if index not in countMap:
                countMap[index] = count
            else:
                countMap[index] = countMap[index] + count
    for index in countMap:
        parallelJoin.append((index,countMap[index]))
    parallelJoin.sort(key = lambda x: x[1])
    return parallelJoin

def writeOutputFile(outputTable, outputPath, cur):
    #Selecting rows from the output table
    query = 'select points_count from ' +  outputTable + ' order by points_count asc'
    cur.execute(query)
    data = cur.fetchall()
    points_count = [str(item[0])+'\n' for item in data]
    
    #We open the file to write the counts
    file_ptr = open(outputPath,"w")
    file_ptr.writelines(points_count)
    file_ptr.close()

def parallelJoin (pointsTable, rectsTable, outputTable, outputPath, openConnection):
    #Implement ParallelJoin Here.
    
    connectCursor = openConnection.cursor()
    point_fragments = pointFragmentation(pointsTable)
    point_fragment_tables = fragmentTables("PointFragTable", point_fragments, connectCursor)
    rectangle_fragments = reactangleFragmentation(rectsTable)
    rectangle_fragments_tables = fragmentTables("RectangleFragTable", rectangle_fragments, connectCursor)
    parallel_joins = [None]*4
    threads = []
    
    for i in range(4):
        threads.append(Thread(target=spatialJoin,args=(point_fragment_tables[i],rectangle_fragments_tables[i],openConnection, parallel_joins, i)))
        
    #Starting the threads
    for thread in threads:
        thread.start()
        
    #Making joins between the fragments
    for thread in threads:
        thread.join()
        
    #Merge parallel joins
    parallel_join = mergeParallelJoin(parallel_joins)
    connectCursor.execute("DROP TABLE IF EXISTS " + outputTable)
    connectCursor.execute("CREATE TABLE " + outputTable + " (rectgeom geometry , points_count INTEGER)")
    
    insert_values = ','.join(['%s'] * len(parallel_join))
    insert_query = "insert into {} (rectgeom, points_count) values {}".format(outputTable,insert_values)
    
    query = connectCursor.mogrify(insert_query, parallel_join).decode('utf8')
    connectCursor.execute(query, query)
    openConnection.commit()
    
    writeOutputFile(outputTable,outputPath,connectCursor)

################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='12345', dbname='dds_assignment2'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='dds_assignment2'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(tablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if tablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            
            # This fetches all the tuples in a table
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (tablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


