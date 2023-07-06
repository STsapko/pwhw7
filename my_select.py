from sqlalchemy import func, desc, and_, distinct, select

from src.models import Teacher, Student, Discipline, Grade, Group
from src.db import session

# Знайти 5 студентів із найбільшим середнім балом з усіх предметів.
def select_1():
    result = session.query(Student.fullname.label('student'), func.round(func.avg(Grade.grade), 2).label('avg_grade'))\
        .select_from(Grade)\
        .join(Student)\
        .group_by(Student.id)\
        .order_by(desc('avg_grade'))\
        .limit(5).all()
    return result    

# Знайти студента з найвищим середнім балом по певному предмету.
def select_2():
    subq = session.query(Discipline.name.label('discipline'), Student.fullname.label('student'), func.avg(Grade.grade).label("avg_grade"))\
        .select_from(Grade)\
        .join(Discipline)\
        .group_by(Discipline.id, Student.id)\
        .subquery()
        
    subq1 = session.query(subq.c.discipline, subq.c.student, 
                  func.row_number().over(partition_by=subq.c.discipline, order_by=desc(subq.c.avg_grade)).label("row_number")
                  )\
        .select_from(subq).subquery()
        
    result = session.query(subq1.c.student, subq1.c.discipline)\
        .select_from(subq1).where(subq1.c.row_number == 1).all()
        
    return result

# Знайти середній бал у групах з певного предмета.
def select_3():
    result = session.query(Group.name.label('group'), Discipline.name.label('discipline'), func.round(func.avg(Grade.grade), 2).label('avg_grade'))\
            .select_from(Group)\
            .join(Student)\
            .join(Grade)\
            .join(Discipline)\
            .group_by(Group.id, Discipline.id).all()

    return result

# Знайти середній бал на потоці (по всій таблиці оцінок).
def select_4():
    result = session.query(func.round(func.avg(Grade.grade), 2).label('avg_grade'))\
        .select_from(Grade).all()
    return result

# Знайти, які курси читає певний викладач.
def select_5():
    result = session.query(Teacher.fullname, Discipline.name)\
        .select_from(Teacher)\
        .join(Discipline)\
        .order_by(Teacher.fullname, Discipline.name).all()
    return result

# Знайти список студентів у певній групі.
def select_6():
    result = session.query(Group.name, Student.fullname)\
        .select_from(Group)\
        .join(Student)\
        .order_by(Group.name, Student.fullname).all()
    return result

# Знайти оцінки студентів в окремій групі з певного предмета.
def select_7():
    result = session.query(Group.name, Student.fullname, Grade.grade)\
        .select_from(Grade)\
        .join(Student)\
        .join(Group)\
        .filter(Group.id == 1)\
        .order_by(Group.name, Student.fullname).all()
    return result

# Знайти середній бал, який ставить певний викладач зі своїх предметів.
def select_8():
    result = session.query(distinct(Teacher.fullname), func.round(func.avg(Grade.grade), 2).label('avg_grade'))\
        .select_from(Grade)\
        .join(Discipline)\
        .join(Teacher)\
        .filter(Teacher.id == 3)\
        .group_by(Teacher.fullname)\
        .order_by(desc('avg_grade'))\
        .all()
    return result
    # filter_by(id = 3)
    
# Знайти список курсів, які відвідує певний студент.
def select_9():
    result = session.query(distinct(Discipline.name))\
        .select_from(Grade)\
        .join(Discipline)\
        .join(Student)\
        .filter(Student.id == 1)\
        .all()
    return result

# Список курсів, які певному студенту читає певний викладач.
def select_10():
    result = session.query(distinct(Discipline.name))\
        .select_from(Grade)\
        .join(Student)\
        .join(Discipline)\
        .join(Teacher)\
        .filter(Student.id == 1, Teacher.id == 2)\
        .all()
    return result

# Середній бал, який певний викладач ставить певному студентові.

def select_11():
    result = session.query(Teacher.fullname, Student.fullname, func.round(func.avg(Grade.grade), 2).label('avg_grade'))\
        .select_from(Grade)\
        .join(Student)\
        .join(Discipline)\
        .join(Teacher)\
        .filter(Student.id == 1, Teacher.id == 2)\
        .group_by(Teacher.id, Student.id)\
        .all()
    return result

if __name__ == '__main__':
    # print(select_1())
    # print(select_2())
    # print(select_3())
    # print(select_4())
    # print(select_5())
    # print(select_6())
    # print(select_7())
    # print(select_8())
    # print(select_9())
    # print(select_10())
    # print(select_11())

    
    
