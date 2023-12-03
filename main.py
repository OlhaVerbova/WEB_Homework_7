import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from pprint import pprint

from sqlalchemy import func, desc, select, and_
from src.models import Teacher, Student, Discipline, Grade, Group
from src.db import session

def select_01():
    """
    Знайти 5 студентів із найбільшим середнім балом з усіх предметів.
    """
    result = session.query(Student.fullname, func.round(func.avg(Grade.grade),2)\
                           .label('avg_grade'))\
                           .select_from(Grade)\
                           .join(Student)\
                           .group_by(Student.id)\
                           .order_by(desc("avg_grade"))\
                           .limit(5).all()
    return result

def select_02(discipline_id:int):
    """
    Знайти студента із найвищим середнім балом з певного предмета.
    """
    result = session.query(Discipline.name, Student.fullname,func.round(func.avg(Grade.grade),2)\
                        .label('avg_grade'))\
                        .select_from(Grade)\
                        .join(Student)\
                        .join(Discipline)\
                        .filter(Discipline.id == discipline_id)\
                        .group_by(Student.id, Discipline.name)\
                        .order_by(desc('avg_grade'))\
                        .limit(1).all()    
                           
    return result

def select_03(discipline_id:int):
    """
    Знайти середній бал у групах з певного предмета.
    """
    result = session.query(Discipline.name, func.round(func.avg(Grade.grade),2)\
                        .label('avg_grade'))\
                        .select_from(Grade)\
                        .join(Discipline)\
                        .filter(Discipline.id == discipline_id)\
                        .group_by(Discipline.name)\
                        .order_by(desc('avg_grade'))\
                        .all()    
                           
    return result

def select_04():
    """
    Знайти середній бал на потоці (по всій таблиці оцінок).
    """
    result = session.query(func.round(func.avg(Grade.grade),2)\
                        .label('avg_grade')).all()                                    
    return result

def select_05(teacher_id:int):
    """
    Знайти які курси читає певний викладач.
    """
    result = session.query(Discipline.name, Teacher.fullname)\
                        .select_from(Teacher)\
                        .join(Discipline)\
                        .filter(Teacher.id == teacher_id)\
                        .all() 
    return result

def select_06(group_id:int):
    """
    Знайти список студентів у певній групі
    """
    result = session.query(Group.name, Student.fullname)\
                        .select_from(Group)\
                        .join(Student)\
                        .filter(Group.id == group_id)\
                        .order_by(Student.fullname)\
                        .all() 
    return result

def select_07(group_id:int, discipline_id:int):
    """
    Знайти оцінки студентів у окремій групі з певного предмета.
    """
    result = session.query(Discipline.name, Student.fullname, Group.name, Grade.grade)\
                        .select_from(Grade)\
                        .join(Discipline)\
                        .join(Student)\
                        .join(Group)\
                        .filter(and_(Discipline.id == discipline_id, Group.id == group_id ))\
                        .all()    
                           
    return result

def select_08(teacher_id:int):
    """
    Знайти середній бал, який ставить певний викладач зі своїх предметів.
    """
    result = session.query(Teacher.fullname,func.round(func.avg(Grade.grade),2)\
                        .label('avg_grade'))\
                        .select_from(Grade)\
                        .join(Discipline)\
                        .join(Teacher)\
                        .filter(Teacher.id == teacher_id)\
                        .group_by(Teacher.fullname)\
                        .all()    
                           
    return result

def select_09(student_id:int):
    """
    Знайти список курсів, які відвідує певний студент.
    """
    result = session.query(Student.fullname, Discipline.name)\
                        .select_from(Grade)\
                        .join(Discipline)\
                        .join(Student)\
                        .filter(Student.id == student_id)\
                        .group_by(Student.fullname, Discipline.name)\
                        .order_by(Student.fullname)\
                        .all()    
                           
    return result

def select_10(student_id:int, teacher_id:int):
    """
    Список курсів, які певному студенту читає певний викладач.
    """
    result = session.query(Student.fullname, Discipline.name, Teacher.fullname)\
                        .select_from(Grade)\
                        .join(Discipline)\
                        .join(Student)\
                        .join(Teacher)\
                        .filter(and_(Student.id == student_id, Teacher.id == teacher_id))\
                        .group_by(Student.fullname, Discipline.name, Teacher.fullname)\
                        .order_by(Discipline.name)\
                        .all()    
                           
    return result

def select_11(teacher_id:int, student_id:int):
    """
    Середній бал, який певний викладач ставить певному студентові.
    """
    result = session.query(Teacher.fullname, Student.fullname, func.round(func.avg(Grade.grade),2)\
                        .label('avg_grade'))\
                        .select_from(Grade)\
                        .join(Discipline)\
                        .join(Teacher)\
                        .join(Student)\
                        .filter(and_(Teacher.id == teacher_id, Student.id == student_id))\
                        .group_by(Teacher.fullname, Student.fullname)\
                        .all() 
    return result

def select_12(discipline_id:int, group_id:int):
    """
    Оцінки студентів у певній групі з певного предмета на останньому занятті.
    """
    subquery = (select(Grade.date_of).join(Student).join(Group)\
                .where(and_(Grade.discipline_id ==discipline_id, Group.id == group_id))\
                .order_by(desc(Grade.date_of)).limit(1)\
                .scalar_subquery())
    result = session.query(Discipline.name, Student.fullname
                         , Group.name, Grade.date_of, Grade.grade)\
                        .select_from(Grade)\
                        .join(Student)\
                        .join(Discipline)\
                        .join(Group)\
                        .filter(and_(Discipline.id == discipline_id, Group.id == group_id, Grade.date_of == subquery))\
                        .order_by(desc(Grade.date_of))\
                        .all()    
    return result

if __name__ == "__main__":
    print(select_01.__doc__)
    pprint(select_01())
    print(select_02.__doc__)
    pprint(select_02(3))
    print(select_03.__doc__)
    pprint(select_03(3))
    print(select_04.__doc__)
    pprint(select_04())
    print(select_05.__doc__)
    pprint(select_05(4))
    print(select_06.__doc__)
    pprint(select_06(1))
    print(select_07.__doc__)
    pprint(select_07(1,2))
    print(select_08.__doc__)
    pprint(select_08(5))
    print(select_09.__doc__)
    pprint(select_09(2))
    print(select_10.__doc__)
    pprint(select_10(1, 5))
    print(select_11.__doc__)
    pprint(select_11(1, 5))
    print(select_12.__doc__)
    pprint(select_12(8, 3))
    


    