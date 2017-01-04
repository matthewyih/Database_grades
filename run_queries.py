import psycopg2



def run_queries():
	query_a()
	print '\n'
	query_b()
	print '\n'
	query_c()
	print '\n'
	query_d()
	print '\n'
	query_e()
	print '\n'
	query_f()
	print '\n'
	query_g()
	print '\n'
	query_5a()
	print '\n'


def terms_all(cursor):
	cursor.execute("""
			SELECT DISTINCT term
			FROM Course;
		""")
	return cursor.fetchall()


def query_a():
	"""
	Calculate the percent of students that attempt 1-20 units of ABC or DEF
	per quarter for every unit increment (e.g. 1, 2, 3).
	"""
	print 'A)'
	connection = psycopg2.connect("dbname=grades")
	cursor = connection.cursor()

	all_terms = terms_all(cursor)
	unit_count_map = {}
	for term in all_terms:
		term = term[0]
		
		cursor.execute("""
				SELECT COUNT(DISTINCT Student.sid)
				FROM Student, Enroll, Course
				WHERE Enroll.cid=Course.cid AND
				   Enroll.sid=Student.sid AND 
				   Enroll.term=Course.term AND
				   Enroll.term=\'{0}\';
			""".format(term))
		per_term_count = cursor.fetchone()[0]
		
		for units in range(1,21):
			cursor.execute("""
					SELECT COUNT(*)
					FROM (SELECT SUM(units_gained) AS sum_units
							FROM Enroll, Course 
							WHERE Enroll.term=\'{0}\' 
									AND Course.cid=Enroll.cid
									AND Course.term=\'{0}\'
									AND (Course.subj='ABC' OR Course.subj='DEF')
							GROUP BY sid) SumUnitsGained
					WHERE SumUnitsGained.sum_units={1};
				""".format(term, units))
			per_term_unit_count = cursor.fetchone()[0]

			if units in unit_count_map:
				unit_count_map[units] += per_term_unit_count / float(per_term_count)
			else:
				unit_count_map[units] = per_term_unit_count / float(per_term_count)

	for key, val in unit_count_map.items():
		print key, (val / len(all_terms))

	cursor.close()
	connection.close()


def query_b():
	"""
	Calculate the average GPA for the students that take each number of units
	from part a. Assume that the grades have standard grade points (A+ = 4.0,
	A = 4.0, A- = 3.7, B+ = 3.3)
	"""
	print 'B)'
	connection = psycopg2.connect("dbname=grades")
	cursor = connection.cursor()
	
	gpa_mapping = {'A+':4.0, 'A':4.0,'A-':3.7,'B+':3.3,'B':3.0,'B-':2.7,'C+':2.3,
	    			'C':2.0,'C-':1.7,'D+':1.3,'D':1.0,'D-':0.7,'F':0.0}
	unit_gpa_map = {}
	gpas_counted_map = {}
	for i in range(1,21):
		unit_gpa_map[i] = 0.0
		gpas_counted_map[i] = 0
		
	all_terms = terms_all(cursor)
	for term in all_terms:
		term = term[0]
		for units in range(1,21):
			cursor.execute("""
				SELECT SumUnitsGained.sid, grade, units_gained
				FROM (SELECT SUM(units_gained) AS sum_units, sid
						FROM Enroll, Course 
						WHERE Enroll.term=\'{0}\' 
								AND Course.cid=Enroll.cid
								AND Course.term=\'{0}\'
						GROUP BY sid) SumUnitsGained,
					(SELECT grade, sid, units_gained
						FROM Enroll, Course 
						WHERE Enroll.term=\'{0}\' 
								AND Course.cid=Enroll.cid
								AND Course.term=\'{0}\'
					) Grade
				WHERE SumUnitsGained.sum_units={1} AND Grade.sid=SumUnitsGained.sid;
			""".format(term, units))

			results = cursor.fetchall()
			gpas_counted_map[units] += len(results)
			for row in results:
				if row[1] in gpa_mapping:
					unit_gpa_map[units] += (gpa_mapping[row[1]] * row[2])
					gpas_counted_map[units] += row[2]


	for i in range(1,21):
		if gpas_counted_map[i] == 0:
			print i, 0
		else:
			print i, unit_gpa_map[i] / gpas_counted_map[i]


	cursor.close()
	connection.close()


def query_c():
	print 'C)'
	connection = psycopg2.connect("dbname=grades")
	cursor = connection.cursor()

	cursor.execute("""
	SELECT Instructor.full_name AS full_name, (SUM( CASE Enroll.grade
												WHEN 'A+' THEN Enroll.units_gained*4
												WHEN 'A' THEN Enroll.units_gained*4
												WHEN 'A-' THEN Enroll.units_gained*3.7
												WHEN 'B+' THEN Enroll.units_gained*3.3
												WHEN 'B' THEN Enroll.units_gained*3
												WHEN 'B-' THEN Enroll.units_gained*2.7
												WHEN 'C+' THEN Enroll.units_gained*2.3
												WHEN 'C' THEN Enroll.units_gained*2
												WHEN 'C-' THEN Enroll.units_gained*1.7
												WHEN 'D+' THEN Enroll.units_gained*1.3
												WHEN 'D' THEN Enroll.units_gained*1
												WHEN 'D-' THEN Enroll.units_gained*0.7
												WHEN 'F' THEN 0
												ELSE 0
												END
											)	/	SUM(CASE Enroll.grade
														WHEN 'A+' THEN Enroll.units_gained
														WHEN 'A' THEN Enroll.units_gained
														WHEN 'A-' THEN Enroll.units_gained
														WHEN 'B+' THEN Enroll.units_gained
														WHEN 'B' THEN Enroll.units_gained
														WHEN 'B-' THEN Enroll.units_gained
														WHEN 'C+' THEN Enroll.units_gained
														WHEN 'C' THEN Enroll.units_gained
														WHEN 'C-' THEN Enroll.units_gained
														WHEN 'D+' THEN Enroll.units_gained
														WHEN 'D' THEN Enroll.units_gained
														WHEN 'D-' THEN Enroll.units_gained
														WHEN 'F' THEN Enroll.units_gained
														ELSE 0.00000001
														END
													)
											) AS averageGPA
	FROM Enroll, Course, Meeting, Instructor
	WHERE Enroll.cid=Course.cid AND
			Enroll.term=Course.term AND
			Meeting.course_id=Course.id AND
			Instructor.id=Meeting.instructor_id
	GROUP BY Instructor.full_name;
	""")
	results = cursor.fetchall()
	min_gpa = 4
	min_teacher = ''
	max_gpa = 0
	max_teacher = ''
	for row in results:
		if row[1] < 0.01:
			continue
		if row[1] > max_gpa:
			max_gpa = row[1]
			max_teacher = row[0]
		if row[1] < min_gpa:
			min_gpa = row[1]
			min_teacher = row[0]

	print "Hardest Overall Instructor: ", min_teacher, round(min_gpa, 2)
	print "Easiest Overall Instructor:", max_teacher, round(max_gpa, 2)

	cursor.close()
	connection.close()


def d_helper(results, max_thing_setter, min_thing_setter):
	course_map = {}
	for row in results:
		if row[2] not in course_map:
			course_map[row[2]] = [(row[0], row[1])]
		else:
			course_map[row[2]].append((row[0], row[1]))

	for key, val in course_map.items():
		print 'Course: ' + str(key)
		min_teacher = ''
		min_thing = min_thing_setter
		max_teacher = ''
		max_thing = max_thing_setter
		for row in val:
			if row[1] > max_thing:
				max_thing = row[1]
				max_teacher = row[0]
			if row[1] < min_thing:
				min_thing = row[1]
				min_teacher = row[0]

		print "Hardest Instructor:", min_teacher, round(min_thing, 2)
		print "Easiest Instructor:", max_teacher, round(max_thing, 2)
		print '\n'


def query_d():
	print 'D)'
	connection = psycopg2.connect("dbname=grades")
	cursor = connection.cursor()

	cursor.execute("""
		SELECT Instructor.full_name AS full_name, SUM( CASE EnrollPNP.grade
													WHEN 'P' THEN 1
													WHEN 'S' THEN 1
													ELSE 0
													END
													) / CAST(COUNT(EnrollPNP.grade) AS FLOAT) AS passrate, Course.crse AS course
		FROM 
			((SELECT *
				FROM Enroll
			)
			EXCEPT
			(SELECT * 
				FROM Enroll 
				WHERE grade LIKE 'A%' OR grade LIKE 'B%' OR grade LIKE 'C%' OR grade LIKE 'D%' OR grade='F'
			)) EnrollPNP, Course, Meeting, Instructor
		WHERE EnrollPNP.cid=Course.cid AND
			EnrollPNP.term=Course.term AND
			Meeting.course_id=Course.id AND
			Instructor.id=Meeting.instructor_id AND
			Course.subj='ABC' AND
			Course.crse LIKE '1%'
		GROUP BY Instructor.full_name, Course.crse;
		""")
	passrate_results = cursor.fetchall()
	for i in range(len(passrate_results)-1,-1,-1):
		if passrate_results[i][1] == 0.0:
			del passrate_results[i]

	d_helper(passrate_results, 0, 1)


	cursor.execute("""
	SELECT Instructor.full_name AS full_name, (SUM( CASE EnrollWOPNP.grade
											WHEN 'A+' THEN EnrollWOPNP.units_gained*4
											WHEN 'A' THEN EnrollWOPNP.units_gained*4
											WHEN 'A-' THEN EnrollWOPNP.units_gained*3.7
											WHEN 'B+' THEN EnrollWOPNP.units_gained*3.3
											WHEN 'B' THEN EnrollWOPNP.units_gained*3
											WHEN 'B-' THEN EnrollWOPNP.units_gained*2.7
											WHEN 'C+' THEN EnrollWOPNP.units_gained*2.3
											WHEN 'C' THEN EnrollWOPNP.units_gained*2
											WHEN 'C-' THEN EnrollWOPNP.units_gained*1.7
											WHEN 'D+' THEN EnrollWOPNP.units_gained*1.3
											WHEN 'D' THEN EnrollWOPNP.units_gained*1
											WHEN 'D-' THEN EnrollWOPNP.units_gained*0.7
											WHEN 'F' THEN 0
											ELSE 0
											END
										)	/	SUM(CASE EnrollWOPNP.grade
													WHEN 'A+' THEN EnrollWOPNP.units_gained
													WHEN 'A' THEN EnrollWOPNP.units_gained
													WHEN 'A-' THEN EnrollWOPNP.units_gained
													WHEN 'B+' THEN EnrollWOPNP.units_gained
													WHEN 'B' THEN EnrollWOPNP.units_gained
													WHEN 'B-' THEN EnrollWOPNP.units_gained
													WHEN 'C+' THEN EnrollWOPNP.units_gained
													WHEN 'C' THEN EnrollWOPNP.units_gained
													WHEN 'C-' THEN EnrollWOPNP.units_gained
													WHEN 'D+' THEN EnrollWOPNP.units_gained
													WHEN 'D' THEN EnrollWOPNP.units_gained
													WHEN 'D-' THEN EnrollWOPNP.units_gained
													WHEN 'F' THEN EnrollWOPNP.units_gained
													ELSE 0
													END
												)
										) AS averageGPA, Course.crse AS course
	FROM (SELECT * 
			FROM Enroll 
			WHERE grade LIKE 'A%' OR grade LIKE 'B%' OR grade LIKE 'C%' OR grade LIKE 'D%' OR grade='F'
		) EnrollWOPNP, Course, Meeting, Instructor
	WHERE EnrollWOPNP.cid=Course.cid AND
			EnrollWOPNP.term=Course.term AND
			Meeting.course_id=Course.id AND
			Instructor.id=Meeting.instructor_id AND
			Course.subj='ABC' AND
			Course.crse LIKE '1%'
	GROUP BY Instructor.full_name, Course.crse;
	""")
	gpa_results = cursor.fetchall()

	d_helper(gpa_results, 0, 4)


	cursor.close()
	connection.close()

	
def query_e():
	print 'E)'

	connection = psycopg2.connect("dbname=grades")
	cursor = connection.cursor()

	cursor.execute("""
		SELECT *
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
		ORDER BY Result.crse1;
	""")
	results = cursor.fetchall()
	for row in results:
		print row

	cursor.close()
	connection.close()

		
def query_f():
	print 'F)'
	
	connection = psycopg2.connect("dbname=grades")
	cursor = connection.cursor()

	cursor.execute("""
	SELECT Enroll.major, (SUM( CASE Enroll.grade
												WHEN 'A+' THEN Enroll.units_gained*4
												WHEN 'A' THEN Enroll.units_gained*4
												WHEN 'A-' THEN Enroll.units_gained*3.7
												WHEN 'B+' THEN Enroll.units_gained*3.3
												WHEN 'B' THEN Enroll.units_gained*3
												WHEN 'B-' THEN Enroll.units_gained*2.7
												WHEN 'C+' THEN Enroll.units_gained*2.3
												WHEN 'C' THEN Enroll.units_gained*2
												WHEN 'C-' THEN Enroll.units_gained*1.7
												WHEN 'D+' THEN Enroll.units_gained*1.3
												WHEN 'D' THEN Enroll.units_gained*1
												WHEN 'D-' THEN Enroll.units_gained*0.7
												WHEN 'F' THEN 0
												ELSE 0
												END
											)	/	SUM(CASE Enroll.grade
														WHEN 'A+' THEN Enroll.units_gained
														WHEN 'A' THEN Enroll.units_gained
														WHEN 'A-' THEN Enroll.units_gained
														WHEN 'B+' THEN Enroll.units_gained
														WHEN 'B' THEN Enroll.units_gained
														WHEN 'B-' THEN Enroll.units_gained
														WHEN 'C+' THEN Enroll.units_gained
														WHEN 'C' THEN Enroll.units_gained
														WHEN 'C-' THEN Enroll.units_gained
														WHEN 'D+' THEN Enroll.units_gained
														WHEN 'D' THEN Enroll.units_gained
														WHEN 'D-' THEN Enroll.units_gained
														WHEN 'F' THEN Enroll.units_gained
														ELSE 0.00000001
														END
													)
											) AS averageGPA
	FROM Enroll, Course
	WHERE Enroll.cid=Course.cid AND
			Enroll.term=Course.term AND
			Course.subj='ABC'
	GROUP BY Enroll.major;
	""")
	results = cursor.fetchall()

	min_gpa = 4
	min_major = ''
	max_gpa = 0
	max_major = ''
	for row in results:
		if row[1] < 0.01:
			continue
		if row[1] > max_gpa:
			max_gpa = row[1]
			max_major = row[0]
		if row[1] < min_gpa:
			min_gpa = row[1]
			min_major = row[0]

	print "Best Performing Major:"
	print max_major, round(max_gpa, 2)
	print "Worst Performing Major:"
	print min_major, round(min_gpa, 2)


	cursor.close()
	connection.close()

	
def query_g():
	
	class temp():
		def __init__(self, percent, major):
			self.percent = percent
			self.major = major

	print 'G)'
	connection = psycopg2.connect("dbname=grades")
	cursor = connection.cursor()

	cursor.execute("""
		SELECT E1.major, Count(DISTINCT E1.sid) AS count
		FROM Enroll E1, Enroll E2
		WHERE 
		E1.sid=E2.sid AND
		E2.major LIKE 'ABC%' AND
		E1.major NOT LIKE 'ABC%' AND
		E2.term>E1.term
		GROUP BY E1.major;
	""")
	results = cursor.fetchall()


	cursor.execute("""
			SELECT COUNT(*)
			FROM Student;
	""")
	all_students_count = cursor.fetchone()[0]

	all_transfer_students = 0
	for r in results:
		all_transfer_students += r[1]
	print 'Percentage of students who transfer into an ABC major: ' + str(float(all_transfer_students) / all_students_count )


	major_percents = []
	for row in results:
		cursor.execute("""
			SELECT COUNT(DISTINCT sid)
			FROM Enroll
			WHERE major=\'{0}\'
		""".format(str(row[0])))
		count = cursor.fetchone()[0]
		
		major_percents.append(temp(row[1] / float(count), row[0]))

	major_percents.sort(key=lambda x: x.percent, reverse=True)
	for i in range(5):
		print major_percents[i].major, major_percents[i].percent 

		
	cursor.close()
	connection.close()


def query_5a():
	from collections import OrderedDict
	import copy
	print "5a)"

	connection = psycopg2.connect("dbname=grades")
	cursor = connection.cursor()
	courses_for_prereq = [('ABC', '203'), ('ABC','210'), ('ABC','222')]

	percentages = OrderedDict([('>95%', []), ('>90%', []), ('>85%', []), ('>80%', []), ('>75%', [])])
	formatted_results = {'ABC203': copy.deepcopy(percentages), 'ABC210': copy.deepcopy(percentages), 'ABC222': copy.deepcopy(percentages)} 

	for elem in courses_for_prereq:
		cursor.execute("""
			SELECT Numerator.subj AS subj, Numerator.crse AS crse, (Numerator.numer / Denominator.denom) AS percent
			FROM (SELECT CE2.subj AS subj, CE2.crse AS crse, count(DISTINCT CE2.sid) AS numer
					FROM (SELECT C1.term AS term, subj, crse, sid
							FROM Course C1, Enroll E1
							WHERE C1.cid=E1.cid AND C1.term=E1.term
							) CE1,
						 (SELECT C2.term AS term, subj, crse, sid
							FROM Course C2, Enroll E2
							WHERE C2.cid=E2.cid AND C2.term=E2.term
							) CE2
					WHERE 
						CE1.subj=\'{0}\' AND
						CE1.crse=\'{1}\' AND
						CE1.sid=CE2.sid AND
						CE1.term>CE2.term
					GROUP BY (CE2.subj,  CE2.crse)
				) Numerator,
				(SELECT CAST(count(DISTINCT CE1.sid) AS FLOAT) AS denom
					FROM (SELECT C1.term AS term, subj, crse, sid
							FROM Course C1, Enroll E1
							WHERE C1.cid=E1.cid AND C1.term=E1.term
							) CE1
					WHERE 
						CE1.subj=\'{0}\' AND
						CE1.crse=\'{1}\'
				) Denominator
			ORDER BY percent DESC;
		""".format(elem[0], elem[1]))
		results = cursor.fetchall()
		for i in range(len(results)):
			if results[i][2] < 0.75:
				results = results[:i]
				break
		
		for row in results:
			if row[2] > 0.95:
				formatted_results[str(elem[0]) + str(elem[1])]['>95%'].append(row)
			elif row[2] > 0.90:
				formatted_results[str(elem[0]) + str(elem[1])]['>90%'].append(row)
			elif row[2] > 0.85:
				formatted_results[str(elem[0]) + str(elem[1])]['>85%'].append(row)
			elif row[2] > 0.80:
				formatted_results[str(elem[0]) + str(elem[1])]['>80%'].append(row)
			elif row[2] > 0.75:
				formatted_results[str(elem[0]) + str(elem[1])]['>75%'].append(row)

	for key, val in formatted_results.items():
		print key + ':'
		for k, v in val.items():
			if len(v) > 0:
				for row in v:
					print k, row[0], row[1], round(row[2], 3)


	cursor.close()
	connection.close()


def main():
	print "Running queries 3a-g and 5a takes ~1 minutes and 15 seconds"
	run_queries()


if __name__ == "__main__":
	main()