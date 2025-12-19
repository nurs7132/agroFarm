# automation.py - АВТОМАТИЧЕСКИЕ ЗАДАЧИ
import psycopg2
import subprocess
import os
from datetime import datetime, timedelta

# Настройки базы данных (такие же как в app.py)
DB_CONFIG = {
    'dbname': 'smart_beef_farm',
    'user': 'postgres', 
    'password': '1234',  
    'host': 'localhost'
}

class FarmAutomation:
    def __init__(self):
        self.conn = psycopg2.connect(**DB_CONFIG)
    
    def update_animal_statuses(self):
        """Автоматическое обновление статусов животных"""
        cursor = self.conn.cursor()
        
        # Животные тяжелее 450 кг готовы к забою
        cursor.execute("""
            UPDATE animals 
            SET status = 'готов к забою' 
            WHERE current_weight >= 450 
            AND status = 'на откорме'
        """)
        
        # Животные легче 300 кг на откорме
        cursor.execute("""
            UPDATE animals 
            SET status = 'на откорме' 
            WHERE current_weight < 450 
            AND status = 'готов к забою'
        """)
        
        self.conn.commit()
        cursor.close()
        print(f"✅ Статусы животных обновлены автоматически")
    
    def check_vaccinations(self):
        """Проверка просроченных вакцинаций"""
        cursor = self.conn.cursor()
        
        cursor.execute("""
            SELECT a.name, v.name, av.next_vaccination_date
            FROM animal_vaccinations av
            JOIN animals a ON av.animal_id = a.id
            JOIN vaccinations v ON av.vaccination_id = v.id
            WHERE av.next_vaccination_date <= CURRENT_DATE + INTERVAL '7 days'
            AND av.next_vaccination_date > CURRENT_DATE
        """)
        upcoming_vaccinations = cursor.fetchall()
        
        cursor.execute("""
            SELECT a.name, v.name, av.next_vaccination_date
            FROM animal_vaccinations av
            JOIN animals a ON av.animal_id = a.id
            JOIN vaccinations v ON av.vaccination_id = v.id
            WHERE av.next_vaccination_date < CURRENT_DATE
        """)
        overdue_vaccinations = cursor.fetchall()
        
        cursor.close()
        
        if upcoming_vaccinations:
            print("⚠️  Ближайшие вакцинации (на этой неделе):")
            for animal, vaccine, date in upcoming_vaccinations:
                print(f"   {animal} - {vaccine} - {date}")
        
        if overdue_vaccinations:
            print("🚨 ПРОСРОЧЕННЫЕ ВАКЦИНАЦИИ:")
            for animal, vaccine, date in overdue_vaccinations:
                print(f"   ❌ {animal} - {vaccine} - был {date}")
        
        return len(upcoming_vaccinations), len(overdue_vaccinations)
    
    def backup_database(self):
        """Создание резервной копии базы данных"""
        backup_dir = "backups"
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = f"{backup_dir}/farm_backup_{timestamp}.sql"
        
        try:
            # Команда для создания бэкапа
            cmd = [
                'pg_dump',
                '-h', DB_CONFIG['host'],
                '-U', DB_CONFIG['user'],
                '-d', DB_CONFIG['dbname'],
                '-f', backup_file
            ]
            
            # Устанавливаем пароль в переменную окружения
            env = os.environ.copy()
            env['PGPASSWORD'] = DB_CONFIG['password']
            
            subprocess.run(cmd, env=env, check=True)
            print(f"✅ Бэкап создан: {backup_file}")
            
            # Удаляем старые бэкапы (оставляем последние 5)
            backups = sorted([f for f in os.listdir(backup_dir) if f.startswith('farm_backup_')])
            if len(backups) > 5:
                for old_backup in backups[:-5]:
                    os.remove(os.path.join(backup_dir, old_backup))
                    print(f"🗑️  Удален старый бэкап: {old_backup}")
                    
        except Exception as e:
            print(f"❌ Ошибка при создании бэкапа: {e}")
    
    def generate_daily_report(self):
        """Генерация ежедневного отчета"""
        cursor = self.conn.cursor()
        
        # Статистика за сегодня
        today = datetime.now().date()
        
        cursor.execute("""
            SELECT 
                COUNT(*) as total_animals,
                COUNT(CASE WHEN status = 'готов к забою' THEN 1 END) as ready_for_slaughter,
                COUNT(CASE WHEN status = 'на откорме' THEN 1 END) as in_feeding
            FROM animals
        """)
        animal_stats = cursor.fetchone()
        
        cursor.execute("""
            SELECT 
                COALESCE(SUM(CASE WHEN type = 'доход' THEN amount ELSE 0 END), 0) as daily_income,
                COALESCE(SUM(CASE WHEN type = 'расход' THEN amount ELSE 0 END), 0) as daily_expense
            FROM finance 
            WHERE date = %s
        """, (today,))
        finance_stats = cursor.fetchone()
        
        cursor.execute("SELECT COUNT(*) FROM tasks WHERE due_date = %s", (today,))
        tasks_today = cursor.fetchone()[0]
        
        cursor.close()
        
        report = {
            'date': today.strftime('%Y-%m-%d'),
            'total_animals': animal_stats[0],
            'ready_for_slaughter': animal_stats[1],
            'in_feeding': animal_stats[2],
            'daily_income': finance_stats[0],
            'daily_expense': finance_stats[1],
            'tasks_today': tasks_today
        }
        
        print("\n📊 ЕЖЕДНЕВНЫЙ ОТЧЕТ:")
        print(f"📅 Дата: {report['date']}")
        print(f"🐄 Животных всего: {report['total_animals']}")
        print(f"🎯 Готово к забою: {report['ready_for_slaughter']}")
        print(f"🌾 На откорме: {report['in_feeding']}")
        print(f"💰 Доход за день: {report['daily_income']} ₸")
        print(f"💸 Расход за день: {report['daily_expense']} ₸")
        print(f"📝 Задач на сегодня: {report['tasks_today']}")
        
        return report

    def update_user_salaries(self):
        """Обновление зарплат пользователей"""
        cursor = self.conn.cursor()
        
        try:
            # Обновляем зарплаты по ролям (админам - 0, менеджерам - 350000, рабочим - 250000)
            cursor.execute("UPDATE users SET salary = 0 WHERE role = 'admin'")
            cursor.execute("UPDATE users SET salary = 350000 WHERE role = 'manager'")
            cursor.execute("UPDATE users SET salary = 250000 WHERE role = 'worker'")
            
            self.conn.commit()
            print("✅ Зарплаты пользователей обновлены")
        except Exception as e:
            print(f"❌ Ошибка при обновлении зарплат: {e}")
        finally:
            cursor.close()
    
    def create_meat_from_slaughtered_animals(self):
        """Автоматическое создание записей о тушах для забитых животных"""
        cursor = self.conn.cursor()
        
        # Находим животных со статусом 'забит', для которых еще нет туш
        cursor.execute("""
            SELECT a.id, a.breed, a.birth_date, a.current_weight, a.name
            FROM animals a
            LEFT JOIN meat_carcasses m ON a.id = m.animal_id
            WHERE a.status = 'забит' AND m.id IS NULL
        """)
        slaughtered_animals = cursor.fetchall()
        
        for animal in slaughtered_animals:
            animal_id, breed, birth_date, weight, name = animal
            
            # Расчет веса туши (примерно 60% от живого веса)
            carcass_weight = round(weight * 0.6, 2)
            
            # Расчет цены (примерно 1500 тенге за кг)
            price = round(carcass_weight * 1500, 2)
            
            try:
                cursor.execute("""
                    INSERT INTO meat_carcasses (animal_id, breed, birth_date, slaughter_date, carcass_weight, price, status, created_by)
                    VALUES (%s, %s, %s, CURRENT_DATE, %s, %s, 'в наличии', 1)
                """, (animal_id, breed, birth_date, carcass_weight, price))
                
                print(f"✅ Создана запись туши для животного: {name}")
                
            except Exception as e:
                print(f"❌ Ошибка при создании туши для {name}: {e}")
        
        self.conn.commit()
        cursor.close()

def main():
    print("🤖 Запуск автоматизации Smart Beef Farm...")
    automation = FarmAutomation()
    
    # Выполняем все автоматические задачи
    automation.update_animal_statuses()
    upcoming, overdue = automation.check_vaccinations()
    automation.backup_database()
    automation.update_user_salaries()
    automation.create_meat_from_slaughtered_animals()
    report = automation.generate_daily_report()
    
    automation.conn.close()
    print("\n✅ Все автоматические задачи выполнены!")

if __name__ == "__main__":
    main()