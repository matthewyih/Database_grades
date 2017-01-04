import psycopg2
import csv
import os



class Cid():
    cid = None 
    term = None 
    subj = None
    crse = None
    sec = None
    min_units = None
    max_units = None
    meetings = []
    seats = []

    def print_me(self):
        print """cid: {0}, term: {1}, subj: {2}, crse: {3}, sec: {4}, units: {5}-{6}, len(meetings): {7}, len(seats): {8}""".format(
                    self.cid, self.term, self.subj, self.crse, self.sec,
                    self.min_units, self.max_units, len(self.meetings), len(self.seats)
                 )

    def __init__(self):
        self.meetings = []
        self.seats = []

    def set_fields(self, row):
        self.cid = (row[0] or 'null')
        self.term = (row[1] or 'null')
        self.subj = row[2] 
        self.crse = row[3]
        self.sec = (row[4] or 'null')
        split_units = row[5].split('-')
        if len(split_units) <= 1:
            if split_units[0] != '' and split_units[0] != None:
                self.min_units = split_units[0]
            else:
                self.min_units = 'null'
            self.max_units = 'null'
        else:
            if split_units[1] == '-':
                import pdb;pdb.set_trace()
            self.min_units = split_units[0]
            self.max_units = split_units[1]


class Meeting():
    instructor = None
    type_field = None
    days = None #list
    time = None
    start_time = 'null'
    end_time = 'null'
    build = None
    room = None

    def set_fields(self, row):
        self.instructor = row[0]
        self.type_field = row[1]
        self.days = row[2]
        self.time = row[3]
        self.build = row[4]
        self.room = (row[5] or 'null')


    def clean_daytime(self):
        time = self.time.split('-')
        if(len(time) > 1):
            self.start_time = time[0].strip()
            self.end_time = time[1].strip()
        self.days = list(self.days)

    def compare_meeting(self, m):
        for key, val in m.__dict__.items():
            if val != '' and val != None:
                if getattr(self, key) != val:
                    return False
            else:
                return False 
        return True

    def set_empty_fields(self, m):
        for key, val in m.__dict__.items():
            if val == '' or val == None:
                setattr(m, key, getattr(self, key))


class Seat():
    seat = None
    sid = None
    surname = None
    prefname = None
    level = None
    units_gained = None
    klass = None 
    major = None
    grade = None
    status = None
    email = None

    def set_fields(self, row):
        self.seat = (row[0] or 'null')
        self.sid = (row[1] or 'null')
        self.surname = row[2]
        self.prefname = row[3]
        self.level = row[4]
        self.units_gained = (row[5] or 'null')
        self.klass = row[6]
        self.major = row[7]
        self.grade = row[8]
        self.status = row[9]
        self.email = row[10]


def insert_student(sid, prefname, surname, email):
    return 'INSERT INTO Student VALUES({0},$unique${1}$unique$,$unique${2}$unique$,$unique${3}$unique$) ON CONFLICT DO NOTHING;'.format(
                sid,prefname,surname,email)


def insert_instructor(full_name):
    #included in other inserts so no semicolon
    return """
            INSERT INTO Instructor VALUES(DEFAULT,$unique${0}$unique$) ON CONFLICT ON CONSTRAINT full_name_unique
            DO UPDATE 
            SET full_name=EXCLUDED.full_name RETURNING id
        """.format(full_name)


def insert_meeting(instructor_id, course_id, room, build, type_field, full_name):
    return """
        WITH inst_id AS ({0})
        INSERT INTO Meeting
        VALUES(DEFAULT, (SELECT id FROM inst_id UNION SELECT id FROM Instructor WHERE full_name=$unique${5}$unique$)
                      ,{1},{2},$unique${3}$unique$,$unique${4}$unique$)
        ON CONFLICT DO NOTHING
        RETURNING id;
        """.format(
                instructor_id,course_id,room,build,type_field,full_name)


def insert_course(term, cid, min_units, max_units, sec, subj, crse):
    #included in other inserts so no semicolon
    return 'INSERT INTO Course VALUES(DEFAULT,\'{0}\',{1},{2},{3},{4},\'{5}\',\'{6}\') RETURNING id'.format(
                term,cid,min_units,max_units,sec,subj,crse)


def insert_enroll(sid, term, cid, seat, units_gained, status, klass, major, level, grade):
    return 'INSERT INTO Enroll VALUES({0},{1},{2},{3},{4},\'{5}\',\'{6}\',\'{7}\',\'{8}\',\'{9}\');'.format(
                sid,term,cid,seat,units_gained,status,klass,major,level,grade)


def insert_daytime(meeting_id, start_time, end_time, day):
    s = 'INSERT INTO DayTime VALUES(DEFAULT,{0}'.format(meeting_id) 
    if start_time == 'null':
        s += ',{0}'.format(start_time)
    else:
        s += ',\'{0}\''.format(start_time)
    if end_time == 'null':
        s += ',{0}'.format(end_time)
    else:
        s += ',\'{0}\''.format(end_time)
    s += ',\'{0}\');'.format(day)
    return s


def insert_roomcap(build, room):
    return 'INSERT INTO RoomCap VALUES(\'{0}\', {1}, null) ON CONFLICT DO NOTHING;'.format(build, room)


def load_data():

    path = os.path.realpath(__file__)
    for i in reversed(range(len(path)-1)):
        if (path[i] == '/'):
            path = path[:i+1]
            break

    os.chdir(path + "Grades")
    for fn in os.listdir(os.getcwd()):
        with open(fn, 'r') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',')
            objects = [] #list of Cids()
            build_objects(objects, filereader)
            clean_data(objects)
            batch_insert_objects(objects)


def create_tables():

    connection = psycopg2.connect("dbname=grades")
    cursor = connection.cursor()

    cursor.execute(
    """
        CREATE TABLE Course(
            id SERIAL,
            term VARCHAR(40) NOT NULL,
            cid INT NOT NULL, 
            min_units INT,
            max_units INT, 
            sec INT,
            subj VARCHAR(3), 
            crse VARCHAR(10), 
            CONSTRAINT term_cid_unique UNIQUE(term, cid),
            PRIMARY KEY (id)
        );

        CREATE TABLE Instructor(
            id SERIAL,
            full_name VARCHAR(100) NOT NULL,
            CONSTRAINT full_name_unique UNIQUE(full_name),
            PRIMARY KEY(id)
        );

        CREATE TABLE Meeting(
            id SERIAL,
            instructor_id INT, 
            course_id INT NOT NULL,
            room INT, 
            build VARCHAR(3),
            type VARCHAR(50), 
            FOREIGN KEY (instructor_id) REFERENCES Instructor(id),
            FOREIGN KEY (course_id) REFERENCES Course(id),
            PRIMARY KEY (id)
        );

        CREATE TABLE DayTime(
            id SERIAL,
            meeting_id INT NOT NULL,
            start_time TIME WITHOUT TIME ZONE,
            end_time TIME WITHOUT TIME ZONE, 
            day VARCHAR(7),
            FOREIGN KEY (meeting_id) REFERENCES Meeting(id),
            PRIMARY KEY (id)
        );

        CREATE TABLE Student(
            sid INT UNIQUE NOT NULL,
            prefname VARCHAR(50),
            surname VARCHAR(50),
            email VARCHAR(320) UNIQUE,
            PRIMARY KEY (sid)
        );

        CREATE TABLE Enroll(
            sid INT NOT NULL,
            term VARCHAR(40) NOT NULL,
            cid INT NOT NULL,
            seat INT,
            units_gained INT,
            status VARCHAR(10),
            class VARCHAR(10),
            major VARCHAR(4),
            level VARCHAR(10),
            grade VARCHAR(10),
            UNIQUE(sid, term, cid),
            FOREIGN KEY (sid) REFERENCES Student(sid),
            FOREIGN KEY (term, cid) REFERENCES Course(term, cid), 
            PRIMARY KEY(sid, cid, term)
        );

        CREATE TABLE RoomCap(
            build VARCHAR(3),
            room INT,
            capacity INT,
            UNIQUE(build, room),
            PRIMARY KEY (build, room)
        );
    """
    )

    connection.commit()

    cursor.close()
    connection.close()


def build_objects(objects, filereader):
    current_header = ''
    current_cid = None
    for row in filereader:
        if (len(row) <= 1):
            continue

        if row[0] == 'CID':
            current_header = 'CID'
            
            if current_cid: #on to the next data group so append to list
                objects.append(current_cid)
            current_cid = Cid() #reset current_cid for next data group
            continue
        elif row[0] == 'INSTRUCTOR(S)':
            current_header = 'INSTRUCTOR(S)'
            continue
        elif row[0] == 'SEAT': 
            current_header = 'SEAT'
            continue

        if current_header == 'CID':
            current_cid.set_fields(row)

        elif current_header == 'INSTRUCTOR(S)':
            m = Meeting()
            m.set_fields(row)
            current_cid.meetings.append(m)

        elif current_header == 'SEAT':
            s = Seat()
            s.set_fields(row)
            current_cid.seats.append(s)

        else:
            print "missed all cases"
    objects.append(current_cid) #append the last object


def clean_data(objects):
    for i in range(len(objects)):
        if len(objects[i].seats) == 0: #empty class
            objects[i].delete = True
            continue
        
        last_meeting = None
        for j in range(len(objects[i].meetings)):
            m = objects[i].meetings[j]
            if not last_meeting:
                last_meeting = objects[i].meetings[j]
                m.clean_daytime()
                continue
            else:
                m.clean_daytime()
                if last_meeting.compare_meeting(m):
                    objects[i].meetings[j].delete = True
                else:
                    last_meeting.set_empty_fields(m)
                last_meeting = objects[i].meetings[j]

        for index in reversed(range( len(objects[i].meetings)-1 )):
            if hasattr(objects[i].meetings[index], 'delete'):
                del objects[i].meetings[index]
    for index in reversed(range( len(objects)-1 )):
        if hasattr(objects[index], 'delete'):
            del objects[index]


def batch_insert_objects(objects):
    """
    order of insert:
        student
        enroll
        instructor and return id
        course return id
        meeting (takes instructor_id and course_id) return id
        daytime (takes meeting_id)
    """
    connection = psycopg2.connect("dbname=grades")
    cursor = connection.cursor()

    batch_insert = ''
    for obj in objects:
        course = insert_course(obj.term, obj.cid, obj.min_units, obj.max_units, obj.sec, obj.subj, obj.crse)
        cursor.execute(course)
        course_id = cursor.fetchone()[0]


        for s in obj.seats:
            batch_insert += insert_student(s.sid, s.prefname, s.surname, s.email)
            batch_insert += insert_enroll(s.sid, obj.term, obj.cid, s.seat, s.units_gained, 
                                          s.status, s.klass, s.major, s.level, s.grade)
        for m in obj.meetings:
            if m.build != '' and m.room != 'null':
                cursor.execute(insert_roomcap(m.build, m.room))

            instructor_id = insert_instructor(m.instructor)
            meeting = insert_meeting(instructor_id, course_id, m.room, m.build, m.type_field, m.instructor)

            cursor.execute(meeting)
            meeting_id = cursor.fetchone()[0]
                
            for day in m.days:
                batch_insert += insert_daytime(meeting_id, m.start_time, m.end_time, day)

    cursor.execute(batch_insert)
    connection.commit()

    cursor.close()
    connection.close()


def calc_roomcap():
    connection = psycopg2.connect("dbname=grades")
    cursor = connection.cursor()


    cursor.execute("""
            SELECT build, room FROM RoomCap;
        """)
    all_rooms = cursor.fetchall()

    insert_string = ''
    for room in all_rooms:
        cursor.execute("""
            SELECT MAX(count_seats)
            FROM (
                   (SELECT COUNT(seat) AS count_seats
                    FROM (SELECT seat, Meeting.id AS mid, build, room, term
                            FROM (SELECT seat, Course.id AS crseid, Course.term AS term
                                    FROM Enroll, Course
                                    WHERE Enroll.term=Course.term AND Enroll.cid=Course.cid AND Enroll.term NOT IN (
                                        SELECT term
                                        FROM (SELECT DISTINCT ON (MCD1.start_time, MCD1.end_time)
                                            MCD1.subj AS subj1, MCD1.crse AS crse1, MCD2.subj AS subj2, MCD2.crse AS crse2, 
                                            MCD1.term AS term
                                            FROM (SELECT term, subj, cid, crse, build, room, day, start_time, end_time, full_name
                                                    FROM (SELECT M1.id, cid, term, build, room, subj, crse, full_name
                                                            FROM Meeting M1, Course C1, Instructor I1
                                                            WHERE C1.id=M1.course_id AND I1.id=M1.instructor_id AND C1.term LIKE '%6'
                                                         ) MC1, DayTime D1
                                                    WHERE MC1.id=D1.meeting_id AND
                                                            D1.start_time IS NOT NULL AND D1.end_time IS NOT NULL 
                                                ) MCD1,
                                                (SELECT term, subj, cid, crse, build, room, day, start_time, end_time, full_name
                                                    FROM (SELECT M2.id, cid, term, build, room, subj, crse, full_name 
                                                            FROM Meeting M2, Course C2, Instructor I2
                                                            WHERE C2.id=M2.course_id AND I2.id=M2.instructor_id AND C2.term LIKE '%6'
                                                         ) MC2, DayTime D2
                                                    WHERE MC2.id=D2.meeting_id AND
                                                            D2.start_time IS NOT NULL AND D2.end_time IS NOT NULL 
                                                ) MCD2
                                            WHERE MCD1.term=MCD2.term AND
                                                MCD1.full_name<>MCD2.full_name AND
                                                MCD1.crse<MCD2.crse AND
                                                MCD1.build=MCD2.build AND
                                                MCD1.room=MCD2.room AND
                                                MCD1.day=MCD2.day AND
                                                MCD1.start_time<MCD2.end_time AND MCD1.end_time>MCD2.start_time) Result
                                        ORDER BY Result.crse1
                                    )
                                ) CE, Meeting
                            WHERE course_id=CE.crseid AND
                                    build=\'{0}\' AND
                                    room={1}
                        ) CEM, DayTime
                    WHERE DayTime.meeting_id=CEM.mid
                    GROUP BY (build, room, term, day, start_time, end_time)
                    )

                    UNION

                    (SELECT COUNT(seat) AS count_seats
                    FROM (SELECT seat, Meeting.id AS mid, build, room, term
                            FROM (SELECT seat, Course.id AS crseid, Course.term AS term
                                    FROM Enroll, Course
                                    WHERE Enroll.term=Course.term AND Enroll.cid=Course.cid AND Enroll.term NOT IN (
                                        SELECT term
                                        FROM (SELECT DISTINCT ON (MCD1.start_time, MCD1.end_time)
                                            MCD1.subj AS subj1, MCD1.crse AS crse1, MCD2.subj AS subj2, MCD2.crse AS crse2, 
                                            MCD1.term AS term
                                            FROM (SELECT term, subj, cid, crse, build, room, day, start_time, end_time, full_name
                                                    FROM (SELECT M1.id, cid, term, build, room, subj, crse, full_name
                                                            FROM Meeting M1, Course C1, Instructor I1
                                                            WHERE C1.id=M1.course_id AND I1.id=M1.instructor_id AND C1.term LIKE '%6'
                                                         ) MC1, DayTime D1
                                                    WHERE MC1.id=D1.meeting_id AND
                                                            D1.start_time IS NOT NULL AND D1.end_time IS NOT NULL 
                                                ) MCD1,
                                                (SELECT term, subj, cid, crse, build, room, day, start_time, end_time, full_name
                                                    FROM (SELECT M2.id, cid, term, build, room, subj, crse, full_name 
                                                            FROM Meeting M2, Course C2, Instructor I2
                                                            WHERE C2.id=M2.course_id AND I2.id=M2.instructor_id AND C2.term LIKE '%6'
                                                         ) MC2, DayTime D2
                                                    WHERE MC2.id=D2.meeting_id AND
                                                            D2.start_time IS NOT NULL AND D2.end_time IS NOT NULL 
                                                ) MCD2
                                            WHERE MCD1.term=MCD2.term AND
                                                MCD1.full_name<>MCD2.full_name AND
                                                MCD1.crse<MCD2.crse AND
                                                MCD1.build=MCD2.build AND
                                                MCD1.room=MCD2.room AND
                                                MCD1.day=MCD2.day AND
                                                MCD1.start_time<MCD2.end_time AND MCD1.end_time>MCD2.start_time) Result
                                        ORDER BY Result.crse1
                                    )
                                ) CE, Meeting
                            WHERE course_id=CE.crseid AND
                                    build=\'{0}\' AND
                                    room={1}
                        ) CEM
                    GROUP BY (build, room, mid, term)
                    )
                 ) UnitedCounts;
        """.format(room[0], room[1]))
        roomcap = cursor.fetchone()[0]
        insert_string += 'UPDATE RoomCap SET capacity={0} WHERE build=\'{1}\' AND room={2};'.format(roomcap, room[0], room[1])
        
    cursor.execute(insert_string)
    connection.commit()

    cursor.close()
    connection.close()


def main():
    print "A full setup takes ~1 minute and 15 seconds"
    create_tables()
    load_data()
    calc_roomcap()


if __name__ == "__main__":
    main()