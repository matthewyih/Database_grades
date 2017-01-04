import psycopg2


def find_open_rooms(term, cid, num_studs):
	print "5b)"
	
	connection = psycopg2.connect("dbname=grades")
	cursor = connection.cursor()

	# let students_enrolled be students currently enrolled in course with given cid and term
	# let expanded_size be num_studs + students_enrolled
	# find all open rooms with capacity >= expandid_size
	# ORDER BY capacity

	cursor.execute("""
		SELECT AllFilled.build AS build, AllFilled.room AS room, AllFilled.cap AS cap, AllFilled.filled as filled
		FROM (
			SELECT MAX(BRCapAndFilled.filled) as filled
			FROM 
				(SELECT RoomCap.build AS ebuild, RoomCap.room AS eroom, RoomCap.capacity AS capacity,
						BRSize.build AS fbuild, BRSize.room AS froom, BRSize.studentNum AS filled
				FROM (  SELECT build, room, MAX(count_seats) AS studentNum
						FROM (
						       (SELECT build, room, COUNT(seat) AS count_seats
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
						                        term=\'{0}\'
						            ) CEM, DayTime
						        WHERE DayTime.meeting_id=CEM.mid
						        GROUP BY (build, room, day, start_time, end_time)
						        )

						        UNION

						        (SELECT build, room, COUNT(seat) AS count_seats
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
						                        term=\'{0}\'
						            ) CEM
						        GROUP BY (build, room, mid)
						        )
						     ) UnitedCounts
						GROUP BY build, room
					) BRSize, RoomCap
				WHERE RoomCap.capacity >= BRSize.studentNum + {2} AND
				      concat(RoomCap.build, RoomCap.room) NOT IN(SELECT concat(Meeting.build, Meeting.room)
									      		     			FROM Meeting, Course
									      		     			WHERE Meeting.course_id=Course.id AND
									      		     			      Course.term=\'{0}\'
									      		     			GROUP BY (Meeting.build, Meeting.room)
									      						)
				) BRCapAndFilled,
				(SELECT build, room, cid
				FROM Meeting, Course
				WHERE Meeting.course_id=Course.id AND Course.term=\'{0}\'
				) BRCid
			WHERE BRCid.cid={1} AND BRCid.build=BRCapAndFilled.fbuild AND BRCid.room=BRCapAndFilled.froom
			) MaxFilled,
			(
			SELECT BRCapAndFilled.ebuild AS build, BRCapAndFilled.eroom AS room, BRCapAndFilled.capacity AS cap, BRCapAndFilled.filled as filled
			FROM 
				(SELECT RoomCap.build AS ebuild, RoomCap.room AS eroom, RoomCap.capacity AS capacity,
						BRSize.build AS fbuild, BRSize.room AS froom, BRSize.studentNum AS filled
				FROM (  SELECT build, room, MAX(count_seats) AS studentNum
						FROM (
						       (SELECT build, room, COUNT(seat) AS count_seats
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
						                        term=\'{0}\'
						            ) CEM, DayTime
						        WHERE DayTime.meeting_id=CEM.mid
						        GROUP BY (build, room, day, start_time, end_time)
						        )

						        UNION

						        (SELECT build, room, COUNT(seat) AS count_seats
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
						                        term=\'{0}\'
						            ) CEM
						        GROUP BY (build, room, mid)
						        )
						     ) UnitedCounts
						GROUP BY build, room
					) BRSize, RoomCap
				WHERE RoomCap.capacity >= BRSize.studentNum + {2} AND
				      concat(RoomCap.build, RoomCap.room) NOT IN(SELECT concat(Meeting.build, Meeting.room)
									      		     			FROM Meeting, Course
									      		     			WHERE Meeting.course_id=Course.id AND
									      		     			      Course.term=\'{0}\'
									      		     			GROUP BY (Meeting.build, Meeting.room)
									      						)
				) BRCapAndFilled,
				(SELECT build, room, cid
				FROM Meeting, Course
				WHERE Meeting.course_id=Course.id AND Course.term=\'{0}\'
				) BRCid
			WHERE BRCid.cid={1} AND BRCid.build=BRCapAndFilled.fbuild AND BRCid.room=BRCapAndFilled.froom
			) AllFilled

		WHERE MaxFilled.filled=AllFilled.filled
		ORDER BY AllFilled.cap;

	""".format(term, cid, num_studs))
	results = cursor.fetchall()
	for row in results[:5]:
		print 'Building-Room: ' + str(row[0]) + '-' + str(row[1]) + ' | Capacity: ' + \
				str(row[2]) + ' | Course has ' + str(row[3]) + ' students enrolled so far.'

	cursor.close()
	connection.close()


def main():
	try:
		term = input("Enter the term: ")
	except:
		term = None
	try:
		cid = input("Enter the cid: ")
	except:
		cid = None
	try:
		num_studs = input("Enter the number of students to add: ")
	except:
		num_studs = None
	if not term or not cid or not num_studs:
		print "Invalid input, try again."
		return
	find_open_rooms(term, cid, num_studs)


if __name__ == '__main__':
	main()

