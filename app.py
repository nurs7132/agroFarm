from functools import wraps
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
import psycopg2
import os
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
from datetime import datetime, timedelta
import json
import re

app = Flask(__name__)
app.secret_key = 'smart_beef_farm_secret_key_2024'
app.config['UPLOAD_FOLDER'] = 'static/uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

if not os.path.exists(app.config['UPLOAD_FOLDER']):
    os.makedirs(app.config['UPLOAD_FOLDER'])

# Настройки базы данных
DB_CONFIG = {
    'dbname': 'smart_beef_farm',
    'user': 'postgres', 
    'password': '1234',
    'host': 'localhost',
    'port': '5432'
}

def get_db_connection():
    """Подключение к базе данных"""
    return psycopg2.connect(**DB_CONFIG)

# Разрешенные расширения для фото
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# ==================== ВАЛИДАЦИЯ ДАННЫХ ====================
def validate_weight(weight):
    """Валидация веса (максимум 1500 кг)"""
    try:
        weight_float = float(weight)
        if weight_float <= 0:
            return False, "Вес должен быть положительным числом"
        if weight_float > 1500:
            return False, "Вес не может превышать 1500 кг"
        return True, "OK"
    except ValueError:
        return False, "Вес должен быть числом"

def validate_text_only(text):
    """Валидация текста (только буквы, пробелы, дефисы)"""
    if not text:
        return True, "OK"
    pattern = r'^[a-zA-Zа-яА-ЯёЁ\s\-]+$'
    if re.match(pattern, text):
        return True, "OK"
    else:
        return False, "Поле должно содержать только буквы, пробелы и дефисы"

def validate_feed_quantity(quantity):
    """Валидация количества корма (максимум 500000 тонн = 500000000 кг)"""
    try:
        quantity_float = float(quantity)
        if quantity_float < 0:
            return False, "Количество не может быть отрицательным"
        if quantity_float > 500000000:
            return False, "Количество корма не может превышать 500000 тонн"
        return True, "OK"
    except ValueError:
        return False, "Количество должно быть числом"

def validate_date_not_future(date_str):
    """Валидация даты (не должна быть в будущем)"""
    if not date_str:
        return True, "OK"
    try:
        input_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        today = datetime.now().date()
        if input_date > today:
            return False, "Дата не может быть в будущем"
        return True, "OK"
    except ValueError:
        return False, "Неверный формат даты"

def validate_date_future(date_str):
    """Валидация даты (должна быть в будущем)"""
    if not date_str:
        return True, "OK"
    try:
        input_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        today = datetime.now().date()
        if input_date <= today:
            return False, "Дата должна быть в будущем"
        return True, "OK"
    except ValueError:
        return False, "Неверный формат даты"

def validate_date_range(date_str, min_year=1980, max_year=2026):
    """Валидация даты в диапазоне годов"""
    if not date_str:
        return True, "OK"
    try:
        input_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        if input_date.year < min_year or input_date.year > max_year:
            return False, f"Год должен быть в диапазоне {min_year}-{max_year}"
        return True, "OK"
    except ValueError:
        return False, "Неверный формат даты"

def validate_positive_number(value, field_name):
    """Валидация положительного числа"""
    try:
        num = float(value)
        if num < 0:
            return False, f"{field_name} не может быть отрицательным"
        return True, "OK"
    except ValueError:
        return False, f"{field_name} должен быть числом"

# ==================== СИСТЕМА ЛОГИРОВАНИЯ ====================
def log_action(action_type, entity_type, entity_id=None, entity_name=None, details=None):
    """Функция для записи действий в лог"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        ip_address = request.remote_addr if request else 'N/A'
        user_agent = request.user_agent.string if request and request.user_agent else 'N/A'
        
        cursor.execute("""
            INSERT INTO action_logs 
            (user_id, username, action_type, entity_type, entity_id, entity_name, details, ip_address, user_agent)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            session.get('user_id'),
            session.get('username', 'unknown'),
            action_type,
            entity_type,
            entity_id,
            entity_name,
            details,
            ip_address,
            user_agent
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Ошибка записи лога: {e}")

# ==================== СИСТЕМА ПРАВ ДОСТУПА ====================
def role_required(roles):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if 'user_id' not in session:
                flash('Требуется авторизация', 'danger')
                return redirect(url_for('login'))
            
            if session.get('role') not in roles:
                flash('Недостаточно прав для выполнения этого действия', 'danger')
                return redirect(url_for('dashboard'))  # ИЗМЕНИЛИ С 'index' НА 'dashboard'
            
            return f(*args, **kwargs)
        return decorated_function
    return decorator

# Декораторы для конкретных ролей
def admin_required(f):
    return role_required(['admin'])(f)

def manager_required(f):
    return role_required(['manager', 'admin'])(f)

def worker_required(f):
    return role_required(['worker', 'manager', 'admin'])(f)


# ==================== ГЛАВНАЯ СТРАНИЦА С ФОРМОЙ ВХОДА ====================
@app.route('/', methods=['GET', 'POST'])
def home():
    """Главная страница с формой входа"""
    if request.method == 'POST':
        # Обработка входа
        username = request.form.get('username')
        password = request.form.get('password')
        
        if not username or not password:
            flash('❌ Пожалуйста, заполните все поля', 'danger')
            return render_template('home.html')
        
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id, username, role, password, full_name FROM users WHERE username = %s", (username,))
        user = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if user and user[3] == password:  # Простая проверка пароля
            session['user_id'] = user[0]
            session['username'] = user[1]
            session['role'] = user[2]
            session['full_name'] = user[4]
            
            # Логируем вход
            log_action('login', 'user', user[0], user[1], f'Вход в систему')
            
            flash(f'✅ Добро пожаловать, {user[1]}!', 'success')
            return redirect(url_for('dashboard'))
        else:
            flash('❌ Неверное имя пользователя или пароль!', 'danger')
    
    return render_template('home.html')


# ==================== СТРАНИЦА "О ПРОГРАММЕ" ====================
@app.route('/about')
def about():
    """Страница "О программе" """
    return render_template('about.html')


# ==================== ВЫХОД ИЗ СИСТЕМЫ ====================
@app.route('/logout')
def logout():
    """Выход из системы"""
    if 'user_id' in session:
        # Логируем выход
        log_action('logout', 'user', session['user_id'], session['username'], f'Выход из системы')
    
    session.clear()
    flash('✅ Вы успешно вышли из системы!', 'success')
    return redirect(url_for('home'))


# ==================== ПАНЕЛЬ УПРАВЛЕНИЯ ====================
@app.route('/dashboard')
@worker_required
def dashboard():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM animals")
    animal_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM animals WHERE status = 'готов к забою'")
    ready_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM fields WHERE status = 'активное'")
    field_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM tasks WHERE status != 'выполнено'")
    task_count = cursor.fetchone()[0]
    
    total_income = None
    total_expense = None
    balance = None
    
    if session.get('role') in ['admin', 'manager']:
        cursor.execute("SELECT COALESCE(SUM(amount), 0) FROM finance WHERE type = 'доход'")
        total_income = cursor.fetchone()[0]
        
        cursor.execute("SELECT COALESCE(SUM(amount), 0) FROM finance WHERE type = 'расход'")
        total_expense = cursor.fetchone()[0]
        
        balance = total_income - total_expense
    
    cursor.execute("""
        SELECT t.title, t.due_date, u.username
        FROM tasks t
        LEFT JOIN users u ON t.assigned_to = u.id
        WHERE t.status != 'выполнено'
        ORDER BY t.due_date
        LIMIT 5
    """)
    recent_tasks = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return render_template('index.html',
                          animal_count=animal_count,
                          ready_count=ready_count,
                          field_count=field_count,
                          task_count=task_count,
                          total_income=total_income,
                          total_expense=total_expense,
                          balance=balance,
                          recent_tasks=recent_tasks)

# ==================== РАЗДЕЛ ЖИВОТНЫХ ====================
@app.route('/animals')
@worker_required
def animals():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            a.id, a.name, a.species, a.breed, a.birth_date, 
            a.current_weight, a.photo, a.status, a.created_by, 
            a.created_at, a.vaccination_type, a.vaccination_date, 
            a.next_vaccination_date, a.vaccination_notes, 
            a.last_weight_update, a.price,
            u.username as created_by_username
        FROM animals a 
        LEFT JOIN users u ON a.created_by = u.id 
        ORDER BY a.created_at DESC
    """)
    animals_data = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return render_template('animals.html', animals=animals_data)

@app.route('/add_animal', methods=['POST'])
@manager_required
def add_animal():
    name = request.form['name']
    species = request.form['species']
    breed = request.form['breed']
    birth_date = request.form['birth_date']
    current_weight = request.form['current_weight']
    status = request.form['status']
    price = request.form.get('price') or None
    
    # Валидация данных
    weight_valid, weight_msg = validate_weight(current_weight)
    if not weight_valid:
        flash(f'❌ {weight_msg}', 'danger')
        return redirect(url_for('animals'))
    
    breed_valid, breed_msg = validate_text_only(breed)
    if not breed_valid:
        flash(f'❌ {breed_msg}', 'danger')
        return redirect(url_for('animals'))
    
    date_valid, date_msg = validate_date_not_future(birth_date)
    if not date_valid:
        flash(f'❌ {date_msg}', 'danger')
        return redirect(url_for('animals'))
    
    # Валидация цены (если статус "готов к забою")
    if status == 'готов к забою' and price:
        price_valid, price_msg = validate_positive_number(price, "Цена")
        if not price_valid:
            flash(f'❌ {price_msg}', 'danger')
            return redirect(url_for('animals'))
    
    photo_filename = None
    if 'photo' in request.files:
        file = request.files['photo']
        if file and file.filename and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            unique_filename = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{filename}"
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], unique_filename))
            photo_filename = unique_filename
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            INSERT INTO animals (name, species, breed, birth_date, current_weight, status, price, photo, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (name, species, breed, birth_date, current_weight, status, price, photo_filename, session['user_id']))
        
        # Получаем ID добавленного животного
        cursor.execute("SELECT LASTVAL()")
        animal_id = cursor.fetchone()[0]
        
        conn.commit()
        
        # Логируем создание животного
        log_action('create', 'animal', animal_id, name, 
                  f'Добавлено животное: {name}, порода: {breed}, вес: {current_weight}кг, статус: {status}')
        
        flash('✅ Животное успешно добавлено!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'❌ Ошибка при добавлении животного: {str(e)}', 'danger')
    finally:
        cursor.close()
        conn.close()
    
    return redirect(url_for('animals'))

@app.route('/add_weight', methods=['POST'])
@manager_required
def add_weight():
    animal_id = request.form['animal_id']
    weight = request.form['weight']
    date = request.form['date']
    
    # Валидация веса
    weight_valid, weight_msg = validate_weight(weight)
    if not weight_valid:
        flash(f'❌ {weight_msg}', 'danger')
        return redirect(url_for('animals'))
    
    # Валидация даты
    date_valid, date_msg = validate_date_not_future(date)
    if not date_valid:
        flash(f'❌ {date_msg}', 'danger')
        return redirect(url_for('animals'))
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            INSERT INTO weights (animal_id, weight, date, measured_by)
            VALUES (%s, %s, %s, %s)
        """, (animal_id, weight, date, session['user_id']))
        
        cursor.execute("""
            UPDATE animals 
            SET current_weight = %s, last_weight_update = %s 
            WHERE id = %s
        """, (weight, date, animal_id))
        
        # Получаем имя животного для лога
        cursor.execute("SELECT name FROM animals WHERE id = %s", (animal_id,))
        animal_name = cursor.fetchone()[0] or "Без имени"
        
        conn.commit()
        
        # Логируем добавление веса
        log_action('update', 'animal', animal_id, animal_name,
                  f'Добавлен вес: {weight}кг для животного {animal_name}')
        
        flash('✅ Вес успешно добавлен!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'❌ Ошибка: {str(e)}', 'danger')
    finally:
        cursor.close()
        conn.close()
    
    return redirect(url_for('animals'))

@app.route('/update_animal_status/<int:animal_id>', methods=['POST'])
@worker_required
def update_animal_status(animal_id):
    new_status = request.form['status']
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Получаем старое значение и имя животного
        cursor.execute("SELECT status, name FROM animals WHERE id = %s", (animal_id,))
        old_data = cursor.fetchone()
        old_status = old_data[0]
        animal_name = old_data[1] or "Без имени"
        
        cursor.execute("UPDATE animals SET status = %s WHERE id = %s", (new_status, animal_id))
        conn.commit()
        
        # Логируем изменение статуса
        log_action('update', 'animal', animal_id, animal_name,
                  f'Изменен статус: {old_status} → {new_status}')
        
        flash('✅ Статус животного обновлен!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'❌ Ошибка: {str(e)}', 'danger')
    finally:
        cursor.close()
        conn.close()
    
    return redirect(url_for('animals'))

@app.route('/delete_animal/<int:animal_id>', methods=['POST'])
@manager_required
def delete_animal(animal_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT name, photo FROM animals WHERE id = %s", (animal_id,))
        animal_data = cursor.fetchone()
        animal_name = animal_data[0] or "Животное"
        photo_filename = animal_data[1]
        
        # Логируем перед удалением
        log_action('delete', 'animal', animal_id, animal_name,
                  f'Удалено животное: {animal_name}')
        
        if photo_filename:
            photo_path = os.path.join(app.config['UPLOAD_FOLDER'], photo_filename)
            if os.path.exists(photo_path):
                os.remove(photo_path)
        
        cursor.execute("DELETE FROM animals WHERE id = %s", (animal_id,))
        conn.commit()
        flash(f'✅ {animal_name} успешно удалено!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'❌ Ошибка при удалении: {str(e)}', 'danger')
    finally:
        cursor.close()
        conn.close()
    
    return redirect(url_for('animals'))

@app.route('/edit_animal/<int:animal_id>', methods=['GET', 'POST'])
@manager_required
def edit_animal(animal_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    if request.method == 'POST':
        # Получаем старые данные для сравнения
        cursor.execute("SELECT name, status, current_weight FROM animals WHERE id = %s", (animal_id,))
        old_data = cursor.fetchone()
        old_name = old_data[0] or "Без имени"
        old_status = old_data[1]
        old_weight = old_data[2]
        
        name = request.form['name']
        species = request.form['species']
        breed = request.form['breed']
        birth_date = request.form['birth_date']
        current_weight = request.form['current_weight']
        status = request.form['status']
        price = request.form.get('price') or None
        vaccination_type = request.form.get('vaccination_type')
        vaccination_date = request.form.get('vaccination_date') or None
        next_vaccination_date = request.form.get('next_vaccination_date') or None
        vaccination_notes = request.form.get('vaccination_notes')
        
        # Валидация данных
        weight_valid, weight_msg = validate_weight(current_weight)
        if not weight_valid:
            flash(f'❌ {weight_msg}', 'danger')
            return redirect(url_for('animals'))
        
        breed_valid, breed_msg = validate_text_only(breed)
        if not breed_valid:
            flash(f'❌ {breed_msg}', 'danger')
            return redirect(url_for('animals'))
        
        date_valid, date_msg = validate_date_not_future(birth_date)
        if not date_valid:
            flash(f'❌ {date_msg}', 'danger')
            return redirect(url_for('animals'))
        
        # Валидация цены (если статус "готов к забою")
        if status == 'готов к забою' and price:
            price_valid, price_msg = validate_positive_number(price, "Цена")
            if not price_valid:
                flash(f'❌ {price_msg}', 'danger')
                return redirect(url_for('animals'))
        
        # Валидация даты вакцинации
        if vaccination_date:
            vacc_date_valid, vacc_date_msg = validate_date_not_future(vaccination_date)
            if not vacc_date_valid:
                flash(f'❌ {vacc_date_msg}', 'danger')
                return redirect(url_for('animals'))
        
        # Валидация следующей даты вакцинации (должна быть в будущем)
        if next_vaccination_date:
            next_vacc_date_valid, next_vacc_date_msg = validate_date_future(next_vaccination_date)
            if not next_vacc_date_valid:
                flash(f'❌ {next_vacc_date_msg}', 'danger')
                return redirect(url_for('animals'))
        
        photo_filename = None
        if 'photo' in request.files:
            file = request.files['photo']
            if file and file.filename and allowed_file(file.filename):
                filename = secure_filename(file.filename)
                unique_filename = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{filename}"
                file.save(os.path.join(app.config['UPLOAD_FOLDER'], unique_filename))
                photo_filename = unique_filename
        
        try:
            if photo_filename:
                cursor.execute("""
                    UPDATE animals 
                    SET name = %s, species = %s, breed = %s, birth_date = %s, 
                        current_weight = %s, status = %s, price = %s, photo = %s,
                        vaccination_type = %s, vaccination_date = %s, 
                        next_vaccination_date = %s, vaccination_notes = %s
                    WHERE id = %s
                """, (name, species, breed, birth_date, current_weight, status, price, photo_filename,
                     vaccination_type, vaccination_date, next_vaccination_date, vaccination_notes, animal_id))
            else:
                cursor.execute("""
                    UPDATE animals 
                    SET name = %s, species = %s, breed = %s, birth_date = %s, 
                        current_weight = %s, status = %s, price = %s,
                        vaccination_type = %s, vaccination_date = %s, 
                        next_vaccination_date = %s, vaccination_notes = %s
                    WHERE id = %s
                """, (name, species, breed, birth_date, current_weight, status, price,
                     vaccination_type, vaccination_date, next_vaccination_date, vaccination_notes, animal_id))
            
            conn.commit()
            
            # Логируем изменения
            changes = []
            if name != old_name:
                changes.append(f"имя: {old_name} → {name}")
            if status != old_status:
                changes.append(f"статус: {old_status} → {status}")
            if float(current_weight) != float(old_weight):
                changes.append(f"вес: {old_weight} → {current_weight}")
            if price:
                changes.append(f"цена: {price}₸")
            
            if changes:
                log_action('update', 'animal', animal_id, name,
                          f'Изменено животное. Изменения: {", ".join(changes)}')
            
            flash('✅ Данные животного обновлены!', 'success')
        except Exception as e:
            conn.rollback()
            flash(f'❌ Ошибка при обновлении: {str(e)}', 'danger')
        finally:
            cursor.close()
            conn.close()
        
        return redirect(url_for('animals'))
    
    else:
        cursor.execute("SELECT * FROM animals WHERE id = %s", (animal_id,))
        animal = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if not animal:
            flash('❌ Животное не найдено!', 'danger')
            return redirect(url_for('animals'))
        
        return render_template('edit_animal.html', animal=animal)

@app.route('/add_vaccination/<int:animal_id>', methods=['POST'])
@manager_required
def add_vaccination(animal_id):
    vaccination_type = request.form['vaccination_type']
    vaccination_date = request.form['vaccination_date'] or None
    next_vaccination_date = request.form.get('next_vaccination_date') or None
    vaccination_notes = request.form.get('vaccination_notes')
    
    # Валидация даты вакцинации
    if vaccination_date:
        vacc_date_valid, vacc_date_msg = validate_date_not_future(vaccination_date)
        if not vacc_date_valid:
            flash(f'❌ {vacc_date_msg}', 'danger')
            return redirect(url_for('animals'))
    
    # Валидация следующей даты вакцинации (должна быть в будущем)
    if next_vaccination_date:
        next_vacc_date_valid, next_vacc_date_msg = validate_date_future(next_vaccination_date)
        if not next_vacc_date_valid:
            flash(f'❌ {next_vacc_date_msg}', 'danger')
            return redirect(url_for('animals'))
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            UPDATE animals 
            SET vaccination_type = %s, vaccination_date = %s, 
                next_vaccination_date = %s, vaccination_notes = %s
            WHERE id = %s
        """, (vaccination_type, vaccination_date, next_vaccination_date, vaccination_notes, animal_id))
        
        # Получаем имя животного для лога
        cursor.execute("SELECT name FROM animals WHERE id = %s", (animal_id,))
        animal_name = cursor.fetchone()[0] or "Без имени"
        
        conn.commit()
        
        # Логируем вакцинацию
        log_action('update', 'animal', animal_id, animal_name,
                  f'Вакцинация: {vaccination_type}, дата: {vaccination_date}')
        
        flash('✅ Информация о вакцинации добавлена!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'❌ Ошибка: {str(e)}', 'danger')
    finally:
        cursor.close()
        conn.close()
    
    return redirect(url_for('animals'))

@app.route('/set_animal_price/<int:animal_id>', methods=['POST'])
@manager_required
def set_animal_price(animal_id):
    """Установка цены для животного готового к забою"""
    price = request.form['price']
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Проверяем, что животное имеет статус "готов к забою"
        cursor.execute("SELECT status, name FROM animals WHERE id = %s", (animal_id,))
        animal = cursor.fetchone()
        
        if not animal:
            flash('❌ Животное не найдено!', 'danger')
            return redirect(url_for('animals'))
        
        if animal[0] != 'готов к забою':
            flash('❌ Цена может быть установлена только для животных, готовых к забою!', 'danger')
            return redirect(url_for('animals'))
        
        animal_name = animal[1] or "Без имени"
        
        # Валидация цены
        price_valid, price_msg = validate_positive_number(price, "Цена")
        if not price_valid:
            flash(f'❌ {price_msg}', 'danger')
            return redirect(url_for('animals'))
        
        cursor.execute("UPDATE animals SET price = %s WHERE id = %s", (price, animal_id))
        conn.commit()
        
        # Логируем установку цены
        log_action('update', 'animal', animal_id, animal_name,
                  f'Установлена цена: {price}₸')
        
        flash('✅ Цена животного успешно установлена!', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'❌ Ошибка при установке цены: {str(e)}', 'danger')
    finally:
        cursor.close()
        conn.close()
    
    return redirect(url_for('animals'))

# ==================== РАЗДЕЛ ФИНАНСОВ ====================
@app.route('/finance')
@manager_required
def finance():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT f.*, u.username, u.role 
        FROM finance f 
        LEFT JOIN users u ON f.created_by = u.id 
        ORDER BY f.date DESC, f.id DESC
    """)
    finance_data = cursor.fetchall()
    
    cursor.execute("SELECT COALESCE(SUM(amount), 0) FROM finance WHERE type = 'доход'")
    total_income = cursor.fetchone()[0] or 0
    
    cursor.execute("SELECT COALESCE(SUM(amount), 0) FROM finance WHERE type = 'расход'")
    total_expense = cursor.fetchone()[0] or 0
    
    balance = total_income - total_expense
    
    cursor.execute("""
        SELECT category, type, SUM(amount) as total 
        FROM finance 
        GROUP BY category, type 
        ORDER BY total DESC
    """)
    category_stats = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return render_template('finance.html', 
                         finance=finance_data,
                         total_income=total_income,
                         total_expense=total_expense,
                         balance=balance,
                         category_stats=category_stats)

@app.route('/add_finance', methods=['POST'])
@manager_required
def add_finance():
    transaction_type = request.form['type']
    category = request.form['category']
    amount = request.form['amount']
    date = request.form['date']
    description = request.form['description']
    
    # Валидация суммы
    amount_valid, amount_msg = validate_positive_number(amount, "Сумма")
    if not amount_valid:
        flash(f'❌ {amount_msg}', 'danger')
        return redirect(url_for('finance'))
    
    # Валидация даты
    date_valid, date_msg = validate_date_not_future(date)
    if not date_valid:
        flash(f'❌ {date_msg}', 'danger')
        return redirect(url_for('finance'))
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            INSERT INTO finance (type, category, amount, date, description, created_by)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (transaction_type, category, amount, date, description, session['user_id']))
        
        # Получаем ID добавленной операции
        cursor.execute("SELECT LASTVAL()")
        finance_id = cursor.fetchone()[0]
        
        conn.commit()
        
        # Логируем создание финансовой операции
        log_action('create', 'finance', finance_id, category,
                  f'Добавлена финансовая операция: {transaction_type}, сумма: {amount}₸, категория: {category}')
        
        flash('✅ Финансовая операция добавлена!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'❌ Ошибка: {str(e)}', 'danger')
    finally:
        cursor.close()
        conn.close()
    
    return redirect(url_for('finance'))

@app.route('/api/finance_charts')
@manager_required
def finance_charts():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT category, SUM(amount) as total 
        FROM finance 
        WHERE type = 'доход'
        GROUP BY category 
        ORDER BY total DESC
    """)
    income_data = cursor.fetchall()
    
    cursor.execute("""
        SELECT category, SUM(amount) as total 
        FROM finance 
        WHERE type = 'расход'
        GROUP BY category 
        ORDER BY total DESC
    """)
    expense_data = cursor.fetchall()
    
    cursor.execute("""
        SELECT 
            TO_CHAR(date, 'YYYY-MM') as month,
            TO_CHAR(date, 'Mon YYYY') as month_name,
            SUM(CASE WHEN type = 'доход' THEN amount ELSE 0 END) as income,
            SUM(CASE WHEN type = 'расход' THEN amount ELSE 0 END) as expense
        FROM finance 
        WHERE date >= CURRENT_DATE - INTERVAL '6 months'
        GROUP BY TO_CHAR(date, 'YYYY-MM'), TO_CHAR(date, 'Mon YYYY')
        ORDER BY month
    """)
    monthly_data = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    income_categories = [row[0] for row in income_data]
    income_amounts = [float(row[1]) for row in income_data]
    
    expense_categories = [row[0] for row in expense_data]
    expense_amounts = [float(row[1]) for row in expense_data]
    
    months = [row[1] for row in monthly_data]
    monthly_income = [float(row[2]) for row in monthly_data]
    monthly_expense = [float(row[3]) for row in monthly_data]
    
    return jsonify({
        'income': {
            'categories': income_categories,
            'amounts': income_amounts
        },
        'expense': {
            'categories': expense_categories,
            'amounts': expense_amounts
        },
        'monthly': {
            'months': months,
            'income': monthly_income,
            'expense': monthly_expense
        }
    })

@app.route('/edit_finance/<int:finance_id>', methods=['GET', 'POST'])
@manager_required
def edit_finance(finance_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    if request.method == 'POST':
        # Получаем старые данные для сравнения
        cursor.execute("SELECT type, category, amount, description FROM finance WHERE id = %s", (finance_id,))
        old_data = cursor.fetchone()
        old_type = old_data[0]
        old_category = old_data[1]
        old_amount = old_data[2]
        old_description = old_data[3] or ""
        
        transaction_type = request.form['type']
        category = request.form['category']
        amount = request.form['amount']
        date = request.form['date']
        description = request.form['description']
        
        # Валидация суммы
        amount_valid, amount_msg = validate_positive_number(amount, "Сумма")
        if not amount_valid:
            flash(f'❌ {amount_msg}', 'danger')
            return redirect(url_for('finance'))
        
        # Валидация даты
        date_valid, date_msg = validate_date_not_future(date)
        if not date_valid:
            flash(f'❌ {date_msg}', 'danger')
            return redirect(url_for('finance'))
        
        try:
            cursor.execute("""
                UPDATE finance 
                SET type=%s, category=%s, amount=%s, date=%s, description=%s
                WHERE id=%s
            """, (transaction_type, category, amount, date, description, finance_id))
            
            conn.commit()
            
            # Логируем изменения
            changes = []
            if transaction_type != old_type:
                changes.append(f"тип: {old_type} → {transaction_type}")
            if category != old_category:
                changes.append(f"категория: {old_category} → {category}")
            if float(amount) != float(old_amount):
                changes.append(f"сумма: {old_amount} → {amount}")
            if description != old_description:
                changes.append("описание изменено")
            
            if changes:
                log_action('update', 'finance', finance_id, category,
                          f'Изменена финансовая операция. Изменения: {", ".join(changes)}')
            
            flash('✅ Финансовая операция обновлена!', 'success')
        except Exception as e:
            conn.rollback()
            flash(f'❌ Ошибка при обновлении: {str(e)}', 'danger')
        finally:
            cursor.close()
            conn.close()
        
        return redirect(url_for('finance'))
    
    else:
        cursor.execute("SELECT * FROM finance WHERE id = %s", (finance_id,))
        finance_operation = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if not finance_operation:
            flash('❌ Финансовая операция не найдена!', 'danger')
            return redirect(url_for('finance'))
        
        return render_template('edit_finance.html', operation=finance_operation)

@app.route('/delete_finance/<int:finance_id>', methods=['POST'])
@manager_required
def delete_finance(finance_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Получаем данные для лога
        cursor.execute("SELECT category, amount FROM finance WHERE id = %s", (finance_id,))
        finance_data = cursor.fetchone()
        category = finance_data[0]
        amount = finance_data[1]
        
        cursor.execute("DELETE FROM finance WHERE id = %s", (finance_id,))
        conn.commit()
        
        # Логируем удаление
        log_action('delete', 'finance', finance_id, category,
                  f'Удалена финансовая операция: {category}, сумма: {amount}₸')
        
        flash('✅ Финансовая операция успешно удалена!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'❌ Ошибка при удалении: {str(e)}', 'danger')
    finally:
        cursor.close()
        conn.close()
    
    return redirect(url_for('finance'))

# ==================== РАЗДЕЛ ЗАДАЧ ====================
@app.route('/tasks')
@worker_required
def tasks():
    status_filter = request.args.get('status')
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Получаем статистику по статусам
    cursor.execute("SELECT status, COUNT(*) FROM tasks GROUP BY status")
    counts = {status: count for status, count in cursor.fetchall()}
    
    # Получаем общее количество
    cursor.execute("SELECT COUNT(*) FROM tasks")
    total_count = cursor.fetchone()[0]
    
    # Получаем задачи с фильтром
    query = """
        SELECT 
            t.id, t.title, t.description, t.due_date, t.priority, t.status,
            t.assigned_to, t.created_by, t.created_at,
            u_assigned.username as assigned_username,
            u_created.username as created_username
        FROM tasks t 
        LEFT JOIN users u_assigned ON t.assigned_to = u_assigned.id 
        LEFT JOIN users u_created ON t.created_by = u_created.id 
    """
    
    if status_filter:
        query += " WHERE t.status = %s "
        params = (status_filter,)
    else:
        params = ()
    
    query += """
        ORDER BY 
            CASE WHEN t.status = 'не начато' THEN 1
                 WHEN t.status = 'в процессе' THEN 2
                 WHEN t.status = 'выполнено' THEN 3
            END,
            CASE WHEN t.priority = 'высокий' THEN 1
                 WHEN t.priority = 'средний' THEN 2
                 WHEN t.priority = 'низкий' THEN 3
            END,
            t.due_date
    """
    
    cursor.execute(query, params)
    tasks_data = cursor.fetchall()
    
    cursor.execute("SELECT id, username FROM users WHERE role IN ('worker', 'manager')")
    users = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return render_template('tasks.html', 
                         tasks=tasks_data, 
                         users=users,
                         counts=counts,
                         total_count=total_count,
                         status_filter=status_filter)

@app.route('/add_task', methods=['POST'])
@manager_required
def add_task():
    title = request.form['title']
    description = request.form['description']
    due_date = request.form['due_date']
    priority = request.form['priority']
    assigned_to = request.form['assigned_to']
    
    # Валидация даты (должна быть в будущем для задач)
    date_valid, date_msg = validate_date_future(due_date)
    if not date_valid:
        flash(f'❌ {date_msg}', 'danger')
        return redirect(url_for('tasks'))
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            INSERT INTO tasks (title, description, due_date, priority, status, assigned_to, created_by)
            VALUES (%s, %s, %s, %s, 'не начато', %s, %s)
        """, (title, description, due_date, priority, assigned_to, session['user_id']))
        
        # Получаем ID добавленной задачи
        cursor.execute("SELECT LASTVAL()")
        task_id = cursor.fetchone()[0]
        
        conn.commit()
        
        # Логируем создание задачи
        log_action('create', 'task', task_id, title,
                  f'Создана задача: {title}, приоритет: {priority}, срок: {due_date}')
        
        flash('✅ Задача успешно создана!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'❌ Ошибка: {str(e)}', 'danger')
    finally:
        cursor.close()
        conn.close()
    
    return redirect(url_for('tasks'))

@app.route('/update_task_status/<int:task_id>', methods=['POST'])
@worker_required
def update_task_status(task_id):
    new_status = request.form['status']
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Получаем старые данные
        cursor.execute("SELECT title, status FROM tasks WHERE id = %s", (task_id,))
        old_data = cursor.fetchone()
        task_title = old_data[0]
        old_status = old_data[1]
        
        cursor.execute("UPDATE tasks SET status = %s WHERE id = %s", (new_status, task_id))
        conn.commit()
        
        # Логируем изменение статуса
        log_action('update', 'task', task_id, task_title,
                  f'Изменен статус задачи: {old_status} → {new_status}')
        
        flash('✅ Статус задачи обновлен!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'❌ Ошибка: {str(e)}', 'danger')
    finally:
        cursor.close()
        conn.close()
    
    return redirect(url_for('tasks'))

@app.route('/delete_task/<int:task_id>', methods=['POST'])
@manager_required
def delete_task(task_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Получаем данные для лога
        cursor.execute("SELECT title FROM tasks WHERE id = %s", (task_id,))
        task_title = cursor.fetchone()[0]
        
        cursor.execute("DELETE FROM tasks WHERE id = %s", (task_id,))
        conn.commit()
        
        # Логируем удаление
        log_action('delete', 'task', task_id, task_title,
                  f'Удалена задача: {task_title}')
        
        flash('✅ Задача успешно удалена!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'❌ Ошибка при удалении: {str(e)}', 'danger')
    finally:
        cursor.close()
        conn.close()
    
    return redirect(url_for('tasks'))

@app.route('/edit_task/<int:task_id>', methods=['POST'])
@manager_required
def edit_task(task_id):
    title = request.form['title']
    description = request.form['description']
    due_date = request.form['due_date']
    priority = request.form['priority']
    status = request.form['status']
    assigned_to = request.form.get('assigned_to') or None
    
    # Валидация даты
    date_valid, date_msg = validate_date_future(due_date)
    if not date_valid and status != 'выполнено':
        flash(f'❌ {date_msg}', 'danger')
        return redirect(url_for('tasks'))
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Получаем старые данные
        cursor.execute("SELECT title, priority, status FROM tasks WHERE id = %s", (task_id,))
        old_data = cursor.fetchone()
        old_title = old_data[0]
        old_priority = old_data[1]
        old_status = old_data[2]
        
        cursor.execute("""
            UPDATE tasks 
            SET title = %s, description = %s, due_date = %s, 
                priority = %s, status = %s, assigned_to = %s
            WHERE id = %s
        """, (title, description, due_date, priority, status, assigned_to, task_id))
        
        conn.commit()
        
        # Логируем изменения
        changes = []
        if title != old_title:
            changes.append(f"название: {old_title} → {title}")
        if priority != old_priority:
            changes.append(f"приоритет: {old_priority} → {priority}")
        if status != old_status:
            changes.append(f"статус: {old_status} → {status}")
        
        if changes:
            log_action('update', 'task', task_id, title,
                      f'Изменена задача. Изменения: {", ".join(changes)}')
        
        flash('✅ Задача успешно обновлена!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'❌ Ошибка: {str(e)}', 'danger')
    finally:
        cursor.close()
        conn.close()
    
    return redirect(url_for('tasks'))

# ==================== РАЗДЕЛ ПОЛЕЙ ====================
@app.route('/fields')
@worker_required
def fields():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM fields ORDER BY name")
    fields_data = cursor.fetchall()
    cursor.close()
    conn.close()
    
    return render_template('fields.html', fields=fields_data)

@app.route('/add_field', methods=['POST'])
@manager_required
def add_field():
    name = request.form['name']
    area = request.form['area']
    crop = request.form['crop']
    last_seeding_date = request.form.get('last_seeding_date') or None
    expected_harvest_date = request.form.get('expected_harvest_date') or None
    status = request.form['status']
    notes = request.form.get('notes')
    
    # Валидация площади
    area_valid, area_msg = validate_positive_number(area, "Площадь")
    if not area_valid:
        flash(f'❌ {area_msg}', 'danger')
        return redirect(url_for('fields'))
    
    # Валидация дат
    if last_seeding_date:
        seeding_date_valid, seeding_date_msg = validate_date_not_future(last_seeding_date)
        if not seeding_date_valid:
            flash(f'❌ {seeding_date_msg}', 'danger')
            return redirect(url_for('fields'))
    
    if expected_harvest_date:
        harvest_date_valid, harvest_date_msg = validate_date_future(expected_harvest_date)
        if not harvest_date_valid:
            flash(f'❌ {harvest_date_msg}', 'danger')
            return redirect(url_for('fields'))
    
    photo_filename = None
    if 'photo' in request.files:
        file = request.files['photo']
        if file and file.filename and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            unique_filename = f"field_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{filename}"
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], unique_filename))
            photo_filename = unique_filename
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            INSERT INTO fields (name, area, crop, last_seeding_date, expected_harvest_date, status, notes, photo)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (name, area, crop, last_seeding_date, expected_harvest_date, status, notes, photo_filename))
        
        # Получаем ID добавленного поля
        cursor.execute("SELECT LASTVAL()")
        field_id = cursor.fetchone()[0]
        
        conn.commit()
        
        # Логируем создание поля
        log_action('create', 'field', field_id, name,
                  f'Добавлено поле: {name}, площадь: {area} га, культура: {crop}')
        
        flash('✅ Поле успешно добавлено!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'❌ Ошибка при добавлении поля: {str(e)}', 'danger')
    finally:
        cursor.close()
        conn.close()
    
    return redirect(url_for('fields'))

@app.route('/edit_field/<int:field_id>', methods=['GET', 'POST'])
@manager_required
def edit_field(field_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    if request.method == 'POST':
        # Получаем старые данные
        cursor.execute("SELECT name, area, crop, status FROM fields WHERE id = %s", (field_id,))
        old_data = cursor.fetchone()
        old_name = old_data[0]
        old_area = old_data[1]
        old_crop = old_data[2]
        old_status = old_data[3]
        
        name = request.form['name']
        area = request.form['area']
        crop = request.form['crop']
        last_seeding_date = request.form.get('last_seeding_date') or None
        expected_harvest_date = request.form.get('expected_harvest_date') or None
        status = request.form['status']
        notes = request.form.get('notes')
        
        # Валидация площади
        area_valid, area_msg = validate_positive_number(area, "Площадь")
        if not area_valid:
            flash(f'❌ {area_msg}', 'danger')
            return redirect(url_for('fields'))
        
        # Валидация дат
        if last_seeding_date:
            seeding_date_valid, seeding_date_msg = validate_date_not_future(last_seeding_date)
            if not seeding_date_valid:
                flash(f'❌ {seeding_date_msg}', 'danger')
                return redirect(url_for('fields'))
        
        if expected_harvest_date:
            harvest_date_valid, harvest_date_msg = validate_date_future(expected_harvest_date)
            if not harvest_date_valid:
                flash(f'❌ {harvest_date_msg}', 'danger')
                return redirect(url_for('fields'))
        
        photo_filename = None
        if 'photo' in request.files:
            file = request.files['photo']
            if file and file.filename and allowed_file(file.filename):
                filename = secure_filename(file.filename)
                unique_filename = f"field_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{filename}"
                file.save(os.path.join(app.config['UPLOAD_FOLDER'], unique_filename))
                photo_filename = unique_filename
        
        try:
            if photo_filename:
                cursor.execute("""
                    UPDATE fields 
                    SET name=%s, area=%s, crop=%s, last_seeding_date=%s, 
                        expected_harvest_date=%s, status=%s, notes=%s, photo=%s
                    WHERE id=%s
                """, (name, area, crop, last_seeding_date, expected_harvest_date, status, notes, photo_filename, field_id))
            else:
                cursor.execute("""
                    UPDATE fields 
                    SET name=%s, area=%s, crop=%s, last_seeding_date=%s, 
                        expected_harvest_date=%s, status=%s, notes=%s
                    WHERE id=%s
                """, (name, area, crop, last_seeding_date, expected_harvest_date, status, notes, field_id))
            
            conn.commit()
            
            # Логируем изменения
            changes = []
            if name != old_name:
                changes.append(f"название: {old_name} → {name}")
            if float(area) != float(old_area):
                changes.append(f"площадь: {old_area} → {area}")
            if crop != old_crop:
                changes.append(f"культура: {old_crop} → {crop}")
            if status != old_status:
                changes.append(f"статус: {old_status} → {status}")
            
            if changes:
                log_action('update', 'field', field_id, name,
                          f'Изменено поле. Изменения: {", ".join(changes)}')
            
            flash('✅ Поле успешно обновлено!', 'success')
        except Exception as e:
            conn.rollback()
            flash(f'❌ Ошибка при обновлении поля: {str(e)}', 'danger')
        finally:
            cursor.close()
            conn.close()
        
        return redirect(url_for('fields'))
    
    else:
        cursor.execute("SELECT * FROM fields WHERE id = %s", (field_id,))
        field = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if not field:
            flash('❌ Поле не найдено!', 'danger')
            return redirect(url_for('fields'))
        
        return render_template('edit_field.html', field=field)

@app.route('/delete_field/<int:field_id>', methods=['POST'])
@manager_required
def delete_field(field_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT name, photo FROM fields WHERE id = %s", (field_id,))
        field_data = cursor.fetchone()
        field_name = field_data[0]
        photo_filename = field_data[1]
        
        # Логируем удаление
        log_action('delete', 'field', field_id, field_name,
                  f'Удалено поле: {field_name}')
        
        if photo_filename:
            photo_path = os.path.join(app.config['UPLOAD_FOLDER'], photo_filename)
            if os.path.exists(photo_path):
                os.remove(photo_path)
        
        cursor.execute("DELETE FROM fields WHERE id = %s", (field_id,))
        conn.commit()
        flash(f'✅ Поле "{field_name}" успешно удалено!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'❌ Ошибка при удалении: {str(e)}', 'danger')
    finally:
        cursor.close()
        conn.close()
    
    return redirect(url_for('fields'))

# ==================== РАЗДЕЛ СКЛАДА ====================
@app.route('/storage')
@worker_required
def storage():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM storage ORDER BY product_type")
    storage_data = cursor.fetchall()
    
    cursor.execute("SELECT * FROM feeding_norms ORDER BY animal_type, feed_type")
    feeding_norms = cursor.fetchall()
    
    cursor.execute("""
        SELECT species, COUNT(*) as count 
        FROM animals 
        WHERE status != 'продан' AND status != 'забит'
        GROUP BY species
    """)
    animals_by_species = cursor.fetchall()
    
    total_daily_consumption = {}
    for norm in feeding_norms:
        animal_type = norm[1]
        feed_type = norm[2]
        daily_norm = norm[3]
        
        animal_count = 0
        for animal in animals_by_species:
            species = animal[0]
            count = animal[1]
            
            if animal_type == 'Крупный рогатый скот' and species in ['бычок', 'корова']:
                animal_count += count
            elif animal_type == 'Овца/баран' and species == 'баран':
                animal_count += count
            elif animal_type == 'Лошадь' and species == 'лошадь':
                animal_count += count
        
        total_consumption = daily_norm * animal_count
        if feed_type not in total_daily_consumption:
            total_daily_consumption[feed_type] = 0
        total_daily_consumption[feed_type] += total_consumption
    
    cursor.close()
    conn.close()
    
    return render_template('storage.html', 
                         storage=storage_data,
                         feeding_norms=feeding_norms,
                         animals_by_species=animals_by_species,
                         total_daily_consumption=total_daily_consumption)

@app.route('/add_feed_type', methods=['POST'])
@manager_required
def add_feed_type():
    product_type = request.form['product_type']
    feed_category = request.form['feed_category']
    unit = request.form['unit']
    min_quantity = request.form['min_quantity']
    price_per_unit = request.form.get('price_per_unit') or 0
    
    # Валидация минимального количества
    min_qty_valid, min_qty_msg = validate_feed_quantity(min_quantity)
    if not min_qty_valid:
        flash(f'❌ {min_qty_msg}', 'danger')
        return redirect(url_for('storage'))
    
    # Валидация цены
    price_valid, price_msg = validate_positive_number(price_per_unit, "Цена")
    if not price_valid:
        flash(f'❌ {price_msg}', 'danger')
        return redirect(url_for('storage'))
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT id FROM storage WHERE product_type = %s", (product_type,))
        if cursor.fetchone():
            flash('❌ Такой тип корма уже существует!', 'danger')
            return redirect(url_for('storage'))
        
        cursor.execute("""
            INSERT INTO storage (product_type, feed_category, unit, min_quantity, price_per_unit, current_quantity)
            VALUES (%s, %s, %s, %s, %s, 0)
        """, (product_type, feed_category, unit, min_quantity, price_per_unit))
        
        # Получаем ID добавленного корма
        cursor.execute("SELECT LASTVAL()")
        feed_id = cursor.fetchone()[0]
        
        conn.commit()
        
        # Логируем добавление типа корма
        log_action('create', 'feed_type', feed_id, product_type,
                  f'Добавлен тип корма: {product_type}, категория: {feed_category}, мин. запас: {min_quantity}{unit}')
        
        flash('✅ Новый тип корма успешно добавлен!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'❌ Ошибка при добавлении типа корма: {str(e)}', 'danger')
    finally:
        cursor.close()
        conn.close()
    
    return redirect(url_for('storage'))

@app.route('/edit_feed_type/<int:feed_id>', methods=['POST'])
@manager_required
def edit_feed_type(feed_id):
    product_type = request.form['product_type']
    feed_category = request.form['feed_category']
    unit = request.form['unit']
    min_quantity = request.form['min_quantity']
    price_per_unit = request.form.get('price_per_unit') or 0
    
    # Валидация минимального количества
    min_qty_valid, min_qty_msg = validate_feed_quantity(min_quantity)
    if not min_qty_valid:
        flash(f'❌ {min_qty_msg}', 'danger')
        return redirect(url_for('storage'))
    
    # Валидация цены
    price_valid, price_msg = validate_positive_number(price_per_unit, "Цена")
    if not price_valid:
        flash(f'❌ {price_msg}', 'danger')
        return redirect(url_for('storage'))
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Получаем старые данные
        cursor.execute("SELECT product_type, feed_category, min_quantity, price_per_unit FROM storage WHERE id = %s", (feed_id,))
        old_data = cursor.fetchone()
        old_product_type = old_data[0]
        old_feed_category = old_data[1]
        old_min_quantity = old_data[2]
        old_price = old_data[3] or 0
        
        cursor.execute("""
            UPDATE storage 
            SET product_type=%s, feed_category=%s, unit=%s, min_quantity=%s, price_per_unit=%s, last_updated=CURRENT_TIMESTAMP
            WHERE id=%s
        """, (product_type, feed_category, unit, min_quantity, price_per_unit, feed_id))
        
        conn.commit()
        
        # Логируем изменения
        changes = []
        if product_type != old_product_type:
            changes.append(f"название: {old_product_type} → {product_type}")
        if feed_category != old_feed_category:
            changes.append(f"категория: {old_feed_category} → {feed_category}")
        if float(min_quantity) != float(old_min_quantity):
            changes.append(f"мин. запас: {old_min_quantity} → {min_quantity}")
        if float(price_per_unit) != float(old_price):
            changes.append(f"цена: {old_price} → {price_per_unit}")
        
        if changes:
            log_action('update', 'feed_type', feed_id, product_type,
                      f'Изменен тип корма. Изменения: {", ".join(changes)}')
        
        flash('✅ Тип корма успешно обновлен!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'❌ Ошибка при обновлении типа корма: {str(e)}', 'danger')
    finally:
        cursor.close()
        conn.close()
    
    return redirect(url_for('storage'))

@app.route('/delete_feed_type/<int:feed_id>', methods=['POST'])
@manager_required
def delete_feed_type(feed_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Получаем данные для лога
        cursor.execute("SELECT product_type FROM storage WHERE id = %s", (feed_id,))
        product_type = cursor.fetchone()[0]
        
        cursor.execute("DELETE FROM storage WHERE id = %s", (feed_id,))
        conn.commit()
        
        # Логируем удаление
        log_action('delete', 'feed_type', feed_id, product_type,
                  f'Удален тип корма: {product_type}')
        
        flash('✅ Тип корма успешно удален!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'❌ Ошибка при удалении типа корма: {str(e)}', 'danger')
    finally:
        cursor.close()
        conn.close()
    
    return redirect(url_for('storage'))

@app.route('/update_storage', methods=['POST'])
@manager_required
def update_storage():
    product_type = request.form['product_type']
    quantity = float(request.form['quantity'])
    operation = request.form['operation']
    
    # Валидация количества корма
    qty_valid, qty_msg = validate_feed_quantity(quantity)
    if not qty_valid:
        flash(f'❌ {qty_msg}', 'danger')
        return redirect(url_for('storage'))
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        if operation == 'add':
            cursor.execute("""
                UPDATE storage 
                SET current_quantity = current_quantity + %s, last_updated = CURRENT_TIMESTAMP
                WHERE product_type = %s
            """, (quantity, product_type))
            
            # Логируем добавление корма
            log_action('update', 'storage', None, product_type,
                      f'Добавлено {quantity} кг {product_type} на склад')
            
            flash(f'✅ Добавлено {quantity} кг {product_type} на склад!', 'success')
        else:
            cursor.execute("""
                UPDATE storage 
                SET current_quantity = %s, last_updated = CURRENT_TIMESTAMP
                WHERE product_type = %s
            """, (quantity, product_type))
            
            # Логируем установку количества
            log_action('update', 'storage', None, product_type,
                      f'Установлено количество {product_type}: {quantity} кг')
            
            flash(f'✅ Количество {product_type} установлено на {quantity} кг!', 'success')
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        flash(f'❌ Ошибка: {str(e)}', 'danger')
    finally:
        cursor.close()
        conn.close()
    
    return redirect(url_for('storage'))

@app.route('/add_feed_consumption', methods=['POST'])
@manager_required
def add_feed_consumption():
    product_type = request.form['product_type']
    quantity = float(request.form['quantity'])
    purpose = request.form['purpose']
    animal_id = request.form.get('animal_id') or None
    consumption_date = request.form['consumption_date']
    notes = request.form.get('notes')
    
    # Валидация количества корма
    qty_valid, qty_msg = validate_feed_quantity(quantity)
    if not qty_valid:
        flash(f'❌ {qty_msg}', 'danger')
        return redirect(url_for('storage'))
    
    # Валидация даты
    date_valid, date_msg = validate_date_not_future(consumption_date)
    if not date_valid:
        flash(f'❌ {date_msg}', 'danger')
        return redirect(url_for('storage'))
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT current_quantity FROM storage WHERE product_type = %s", (product_type,))
        current_qty = cursor.fetchone()
        
        if not current_qty or current_qty[0] < quantity:
            flash(f'❌ Недостаточно {product_type} на складе!', 'danger')
            return redirect(url_for('storage'))
        
        cursor.execute("""
            INSERT INTO feed_consumption (product_type, quantity, purpose, animal_id, consumption_date, recorded_by, notes)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (product_type, quantity, purpose, animal_id, consumption_date, session['user_id'], notes))
        
        cursor.execute("""
            UPDATE storage 
            SET current_quantity = current_quantity - %s, last_updated = CURRENT_TIMESTAMP
            WHERE product_type = %s
        """, (quantity, product_type))
        
        # Получаем ID операции списания
        cursor.execute("SELECT LASTVAL()")
        consumption_id = cursor.fetchone()[0]
        
        conn.commit()
        
        # Логируем списание корма
        log_action('create', 'feed_consumption', consumption_id, product_type,
                  f'Списание корма: {product_type}, количество: {quantity} кг, назначение: {purpose}')
        
        flash('✅ Расход корма успешно записан!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'❌ Ошибка: {str(e)}', 'danger')
    finally:
        cursor.close()
        conn.close()
    
    return redirect(url_for('storage'))

# ==================== РАЗДЕЛ ТЕХНИКИ ====================
@app.route('/machinery')
@worker_required
def machinery():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM machines ORDER BY type, model")
    machinery_data = cursor.fetchall()
    cursor.close()
    conn.close()
    
    return render_template('machinery.html', machinery=machinery_data)

@app.route('/add_machine', methods=['POST'])
@manager_required
def add_machine():
    machine_type = request.form['type']
    model = request.form['model']
    serial_number = request.form.get('serial_number')
    purchase_date = request.form.get('purchase_date') or None
    condition = request.form['condition']
    last_service_date = request.form.get('last_service_date') or None
    next_service_date = request.form.get('next_service_date') or None
    service_notes = request.form.get('service_notes')
    
    # Валидация дат
    if purchase_date:
        purchase_date_valid, purchase_date_msg = validate_date_not_future(purchase_date)
        if not purchase_date_valid:
            flash(f'❌ {purchase_date_msg}', 'danger')
            return redirect(url_for('machinery'))
    
    if last_service_date:
        last_service_date_valid, last_service_date_msg = validate_date_not_future(last_service_date)
        if not last_service_date_valid:
            flash(f'❌ {last_service_date_msg}', 'danger')
            return redirect(url_for('machinery'))
    
    if next_service_date:
        next_service_date_valid, next_service_date_msg = validate_date_future(next_service_date)
        if not next_service_date_valid:
            flash(f'❌ {next_service_date_msg}', 'danger')
            return redirect(url_for('machinery'))
    
    photo_filename = None
    if 'photo' in request.files:
        file = request.files['photo']
        if file and file.filename and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            unique_filename = f"machine_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{filename}"
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], unique_filename))
            photo_filename = unique_filename
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            INSERT INTO machines (type, model, serial_number, purchase_date, condition, 
                                last_service_date, next_service_date, service_notes, photo, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (machine_type, model, serial_number, purchase_date, condition, 
              last_service_date, next_service_date, service_notes, photo_filename, session['user_id']))
        
        # Получаем ID добавленной техники
        cursor.execute("SELECT LASTVAL()")
        machine_id = cursor.fetchone()[0]
        
        conn.commit()
        
        # Логируем добавление техники
        log_action('create', 'machine', machine_id, model,
                  f'Добавлена техника: {model}, тип: {machine_type}, состояние: {condition}')
        
        flash('✅ Техника успешно добавлена!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'❌ Ошибка при добавлении техники: {str(e)}', 'danger')
    finally:
        cursor.close()
        conn.close()
    
    return redirect(url_for('machinery'))

@app.route('/edit_machine/<int:machine_id>', methods=['POST'])
@manager_required
def edit_machine(machine_id):
    machine_type = request.form['type']
    model = request.form['model']
    serial_number = request.form.get('serial_number')
    purchase_date = request.form.get('purchase_date') or None
    condition = request.form['condition']
    last_service_date = request.form.get('last_service_date') or None
    next_service_date = request.form.get('next_service_date') or None
    service_notes = request.form.get('service_notes')
    
    # Валидация дат
    if purchase_date:
        purchase_date_valid, purchase_date_msg = validate_date_not_future(purchase_date)
        if not purchase_date_valid:
            flash(f'❌ {purchase_date_msg}', 'danger')
            return redirect(url_for('machinery'))
    
    if last_service_date:
        last_service_date_valid, last_service_date_msg = validate_date_not_future(last_service_date)
        if not last_service_date_valid:
            flash(f'❌ {last_service_date_msg}', 'danger')
            return redirect(url_for('machinery'))
    
    if next_service_date:
        next_service_date_valid, next_service_date_msg = validate_date_future(next_service_date)
        if not next_service_date_valid:
            flash(f'❌ {next_service_date_msg}', 'danger')
            return redirect(url_for('machinery'))
    
    photo_filename = None
    if 'photo' in request.files:
        file = request.files['photo']
        if file and file.filename and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            unique_filename = f"machine_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{filename}"
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], unique_filename))
            photo_filename = unique_filename
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Получаем старые данные
        cursor.execute("SELECT model, condition FROM machines WHERE id = %s", (machine_id,))
        old_data = cursor.fetchone()
        old_model = old_data[0]
        old_condition = old_data[1]
        
        if photo_filename:
            cursor.execute("""
                UPDATE machines 
                SET type=%s, model=%s, serial_number=%s, purchase_date=%s, condition=%s, 
                    last_service_date=%s, next_service_date=%s, service_notes=%s, photo=%s
                WHERE id=%s
            """, (machine_type, model, serial_number, purchase_date, condition, 
                  last_service_date, next_service_date, service_notes, photo_filename, machine_id))
        else:
            cursor.execute("""
                UPDATE machines 
                SET type=%s, model=%s, serial_number=%s, purchase_date=%s, condition=%s, 
                    last_service_date=%s, next_service_date=%s, service_notes=%s
                WHERE id=%s
            """, (machine_type, model, serial_number, purchase_date, condition, 
                  last_service_date, next_service_date, service_notes, machine_id))
        
        conn.commit()
        
        # Логируем изменения
        changes = []
        if model != old_model:
            changes.append(f"модель: {old_model} → {model}")
        if condition != old_condition:
            changes.append(f"состояние: {old_condition} → {condition}")
        
        if changes:
            log_action('update', 'machine', machine_id, model,
                      f'Изменена техника. Изменения: {", ".join(changes)}')
        
        flash('✅ Данные техники обновлены!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'❌ Ошибка при обновлении техники: {str(e)}', 'danger')
    finally:
        cursor.close()
        conn.close()
    
    return redirect(url_for('machinery'))

@app.route('/update_machine_condition/<int:machine_id>', methods=['POST'])
@worker_required
def update_machine_condition(machine_id):
    new_condition = request.form['condition']
    service_notes = request.form.get('service_notes')
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Получаем старые данные
        cursor.execute("SELECT model, condition FROM machines WHERE id = %s", (machine_id,))
        old_data = cursor.fetchone()
        model = old_data[0]
        old_condition = old_data[1]
        
        cursor.execute("""
            UPDATE machines 
            SET condition = %s, service_notes = %s 
            WHERE id = %s
        """, (new_condition, service_notes, machine_id))
        
        conn.commit()
        
        # Логируем изменение состояния
        log_action('update', 'machine', machine_id, model,
                  f'Изменено состояние техники: {old_condition} → {new_condition}')
        
        flash('✅ Состояние техники обновлено!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'❌ Ошибка: {str(e)}', 'danger')
    finally:
        cursor.close()
        conn.close()
    
    return redirect(url_for('machinery'))

@app.route('/update_service_dates/<int:machine_id>', methods=['POST'])
@manager_required
def update_service_dates(machine_id):
    last_service_date = request.form.get('last_service_date') or None
    next_service_date = request.form.get('next_service_date') or None
    
    # Валидация дат
    if last_service_date:
        last_service_date_valid, last_service_date_msg = validate_date_not_future(last_service_date)
        if not last_service_date_valid:
            flash(f'❌ {last_service_date_msg}', 'danger')
            return redirect(url_for('machinery'))
    
    if next_service_date:
        next_service_date_valid, next_service_date_msg = validate_date_future(next_service_date)
        if not next_service_date_valid:
            flash(f'❌ {next_service_date_msg}', 'danger')
            return redirect(url_for('machinery'))
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT model FROM machines WHERE id = %s", (machine_id,))
        model = cursor.fetchone()[0]
        
        cursor.execute("""
            UPDATE machines 
            SET last_service_date = %s, next_service_date = %s 
            WHERE id = %s
        """, (last_service_date, next_service_date, machine_id))
        
        conn.commit()
        
        # Логируем обновление дат ТО
        log_action('update', 'machine', machine_id, model,
                  f'Обновлены даты ТО: последнее ТО: {last_service_date}, следующее ТО: {next_service_date}')
        
        flash('✅ Даты ТО обновлены!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'❌ Ошибка: {str(e)}', 'danger')
    finally:
        cursor.close()
        conn.close()
    
    return redirect(url_for('machinery'))

@app.route('/delete_machine/<int:machine_id>', methods=['POST'])
@manager_required
def delete_machine(machine_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT model, photo FROM machines WHERE id = %s", (machine_id,))
        machine_data = cursor.fetchone()
        machine_model = machine_data[0] or "Техника"
        photo_filename = machine_data[1]
        
        # Логируем удаление
        log_action('delete', 'machine', machine_id, machine_model,
                  f'Удалена техника: {machine_model}')
        
        if photo_filename:
            photo_path = os.path.join(app.config['UPLOAD_FOLDER'], photo_filename)
            if os.path.exists(photo_path):
                os.remove(photo_path)
        
        cursor.execute("DELETE FROM machines WHERE id = %s", (machine_id,))
        conn.commit()
        flash(f'✅ {machine_model} успешно удалена!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'❌ Ошибка при удалении: {str(e)}', 'danger')
    finally:
        cursor.close()
        conn.close()
    
    return redirect(url_for('machinery'))


# ==================== API ДЛЯ ГРАФИКОВ ====================
@app.route('/api/animal_stats')
@worker_required
def animal_stats():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT breed, COUNT(*) as count, AVG(current_weight) as avg_weight
        FROM animals 
        GROUP BY breed
    """)
    breed_stats = cursor.fetchall()
    
    cursor.execute("""
        SELECT a.name, w.date, w.weight 
        FROM weights w 
        JOIN animals a ON w.animal_id = a.id 
        ORDER BY a.name, w.date
    """)
    weight_history = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    breeds = [row[0] for row in breed_stats]
    counts = [row[1] for row in breed_stats]
    avg_weights = [float(row[2]) if row[2] else 0 for row in breed_stats]
    
    return jsonify({
        'breeds': breeds,
        'counts': counts,
        'avg_weights': avg_weights,
        'weight_history': [
            {'animal': row[0], 'date': row[1].strftime('%Y-%m-%d'), 'weight': float(row[2])} 
            for row in weight_history
        ]
    })

# ==================== РАЗДЕЛ МЯСА ====================
@app.route('/meat')
@worker_required
def meat():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT m.*, u.username as created_by_username, a.name as animal_name
        FROM meat_carcasses m
        LEFT JOIN users u ON m.created_by = u.id
        LEFT JOIN animals a ON m.animal_id = a.id
        ORDER BY m.slaughter_date DESC, m.created_at DESC
    """)
    meat_data = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return render_template('meat.html', meat=meat_data)

@app.route('/add_meat', methods=['POST'])
@manager_required
def add_meat():
    animal_id = request.form.get('animal_id') or None
    breed = request.form['breed']
    birth_date = request.form.get('birth_date') or None
    slaughter_date = request.form['slaughter_date']
    carcass_weight = request.form['carcass_weight']
    price = request.form['price']
    description = request.form.get('description')
    status = request.form['status']
    
    # Валидация породы
    breed_valid, breed_msg = validate_text_only(breed)
    if not breed_valid:
        flash(f'❌ {breed_msg}', 'danger')
        return redirect(url_for('meat'))
    
    # Валидация веса туши
    weight_valid, weight_msg = validate_weight(carcass_weight)
    if not weight_valid:
        flash(f'❌ {weight_msg}', 'danger')
        return redirect(url_for('meat'))
    
    # Валидация цены
    price_valid, price_msg = validate_positive_number(price, "Цена")
    if not price_valid:
        flash(f'❌ {price_msg}', 'danger')
        return redirect(url_for('meat'))
    
    # Валидация дат
    if birth_date:
        birth_date_valid, birth_date_msg = validate_date_not_future(birth_date)
        if not birth_date_valid:
            flash(f'❌ {birth_date_msg}', 'danger')
            return redirect(url_for('meat'))
    
    slaughter_date_valid, slaughter_date_msg = validate_date_not_future(slaughter_date)
    if not slaughter_date_valid:
        flash(f'❌ {slaughter_date_msg}', 'danger')
        return redirect(url_for('meat'))
    
    photo_filename = None
    if 'photo' in request.files:
        file = request.files['photo']
        if file and file.filename and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            unique_filename = f"meat_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{filename}"
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], unique_filename))
            photo_filename = unique_filename
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            INSERT INTO meat_carcasses (animal_id, breed, birth_date, slaughter_date, carcass_weight, price, description, photo, status, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (animal_id, breed, birth_date, slaughter_date, carcass_weight, price, description, photo_filename, status, session['user_id']))
        
        # Получаем ID добавленной туши
        cursor.execute("SELECT LASTVAL()")
        meat_id = cursor.fetchone()[0]
        
        conn.commit()
        
        # Логируем добавление туши
        log_action('create', 'meat', meat_id, breed,
                  f'Добавлена туша: порода: {breed}, вес: {carcass_weight}кг, цена: {price}₸')
        
        flash('✅ Туша успешно добавлена!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'❌ Ошибка при добавлении туши: {str(e)}', 'danger')
    finally:
        cursor.close()
        conn.close()
    
    return redirect(url_for('meat'))

@app.route('/edit_meat/<int:meat_id>', methods=['POST'])
@manager_required
def edit_meat(meat_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    if request.method == 'POST':
        # Получаем старые данные
        cursor.execute("SELECT breed, carcass_weight, price, status FROM meat_carcasses WHERE id = %s", (meat_id,))
        old_data = cursor.fetchone()
        old_breed = old_data[0]
        old_weight = old_data[1]
        old_price = old_data[2]
        old_status = old_data[3]
        
        animal_id = request.form.get('animal_id') or None
        breed = request.form['breed']
        birth_date = request.form.get('birth_date') or None
        slaughter_date = request.form['slaughter_date']
        
        try:
            carcass_weight = float(request.form['carcass_weight'])
            price = float(request.form['price'])
        except ValueError:
            flash('❌ Ошибка: вес и цена должны быть числами!', 'danger')
            return redirect(url_for('meat'))
        
        # Валидация породы
        breed_valid, breed_msg = validate_text_only(breed)
        if not breed_valid:
            flash(f'❌ {breed_msg}', 'danger')
            return redirect(url_for('meat'))
        
        # Валидация веса туши
        weight_valid, weight_msg = validate_weight(carcass_weight)
        if not weight_valid:
            flash(f'❌ {weight_msg}', 'danger')
            return redirect(url_for('meat'))
        
        # Валидация цены
        price_valid, price_msg = validate_positive_number(price, "Цена")
        if not price_valid:
            flash(f'❌ {price_msg}', 'danger')
            return redirect(url_for('meat'))
        
        # Валидация дат
        if birth_date:
            birth_date_valid, birth_date_msg = validate_date_not_future(birth_date)
            if not birth_date_valid:
                flash(f'❌ {birth_date_msg}', 'danger')
                return redirect(url_for('meat'))
        
        slaughter_date_valid, slaughter_date_msg = validate_date_not_future(slaughter_date)
        if not slaughter_date_valid:
            flash(f'❌ {slaughter_date_msg}', 'danger')
            return redirect(url_for('meat'))
        
        description = request.form.get('description')
        status = request.form['status']
        
        photo_filename = None
        if 'photo' in request.files:
            file = request.files['photo']
            if file and file.filename and allowed_file(file.filename):
                filename = secure_filename(file.filename)
                unique_filename = f"meat_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{filename}"
                file.save(os.path.join(app.config['UPLOAD_FOLDER'], unique_filename))
                photo_filename = unique_filename
        
        try:
            if photo_filename:
                cursor.execute("""
                    UPDATE meat_carcasses 
                    SET animal_id=%s, breed=%s, birth_date=%s, slaughter_date=%s, 
                        carcass_weight=%s, price=%s, description=%s, photo=%s, status=%s
                    WHERE id=%s
                """, (animal_id, breed, birth_date, slaughter_date, carcass_weight, price, description, photo_filename, status, meat_id))
            else:
                cursor.execute("""
                    UPDATE meat_carcasses 
                    SET animal_id=%s, breed=%s, birth_date=%s, slaughter_date=%s, 
                        carcass_weight=%s, price=%s, description=%s, status=%s
                    WHERE id=%s
                """, (animal_id, breed, birth_date, slaughter_date, carcass_weight, price, description, status, meat_id))
            
            conn.commit()
            
            # Логируем изменения
            changes = []
            if breed != old_breed:
                changes.append(f"порода: {old_breed} → {breed}")
            if carcass_weight != old_weight:
                changes.append(f"вес: {old_weight} → {carcass_weight}")
            if price != old_price:
                changes.append(f"цена: {old_price} → {price}")
            if status != old_status:
                changes.append(f"статус: {old_status} → {status}")
            
            if changes:
                log_action('update', 'meat', meat_id, breed,
                          f'Изменена туша. Изменения: {", ".join(changes)}')
            
            flash('✅ Данные туши обновлены!', 'success')
        except Exception as e:
            conn.rollback()
            flash(f'❌ Ошибка при обновлении: {str(e)}', 'danger')
        finally:
            cursor.close()
            conn.close()
        
        return redirect(url_for('meat'))

@app.route('/delete_meat/<int:meat_id>', methods=['POST'])
@manager_required
def delete_meat(meat_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT breed, photo FROM meat_carcasses WHERE id = %s", (meat_id,))
        meat_data = cursor.fetchone()
        breed = meat_data[0]
        photo_filename = meat_data[1]
        
        # Логируем удаление
        log_action('delete', 'meat', meat_id, breed,
                  f'Удалена туша: {breed}')
        
        if photo_filename:
            photo_path = os.path.join(app.config['UPLOAD_FOLDER'], photo_filename)
            if os.path.exists(photo_path):
                os.remove(photo_path)
        
        cursor.execute("DELETE FROM meat_carcasses WHERE id = %s", (meat_id,))
        conn.commit()
        flash('✅ Туша успешно удалена!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'❌ Ошибка при удалении: {str(e)}', 'danger')
    finally:
        cursor.close()
        conn.close()
    
    return redirect(url_for('meat'))

@app.route('/update_meat_status/<int:meat_id>', methods=['POST'])
@manager_required
def update_meat_status(meat_id):
    new_status = request.form['status']
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Получаем старые данные
        cursor.execute("SELECT breed, status FROM meat_carcasses WHERE id = %s", (meat_id,))
        old_data = cursor.fetchone()
        breed = old_data[0]
        old_status = old_data[1]
        
        cursor.execute("UPDATE meat_carcasses SET status = %s WHERE id = %s", (new_status, meat_id))
        conn.commit()
        
        # Логируем изменение статуса
        log_action('update', 'meat', meat_id, breed,
                  f'Изменен статус туши: {old_status} → {new_status}')
        
        flash('✅ Статус туши обновлен!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'❌ Ошибка: {str(e)}', 'danger')
    finally:
        cursor.close()
        conn.close()
    
    return redirect(url_for('meat'))

# ==================== РАЗДЕЛ ЗАКАЗОВ ====================
@app.route('/orders')
@manager_required
def orders():
    status_filter = request.args.get('status', 'все')
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Получаем статистику по статусам
    cursor.execute("""
        SELECT status, COUNT(*) as count 
        FROM orders 
        GROUP BY status
    """)
    status_counts = cursor.fetchall()
    stats = {status: count for status, count in status_counts}
    
    # Определяем колонки для выборки
    columns = [
        'id', 'customer_name', 'phone', 'telegram_username',
        'order_type', 'product_id', 'product_name', 
        'quantity', 'price', 'total_price', 'status',
        'notes', 'created_at', 'updated_at', 'created_by'
    ]
    
    # Получаем заказы с фильтром
    if status_filter == 'все':
        cursor.execute(f"""
            SELECT {', '.join(columns)}
            FROM orders 
            ORDER BY created_at DESC
        """)
    else:
        cursor.execute(f"""
            SELECT {', '.join(columns)}
            FROM orders 
            WHERE status = %s 
            ORDER BY created_at DESC
        """, (status_filter,))
    
    orders_data = cursor.fetchall()
    
    # Обрабатываем даты
    processed_orders = []
    for order in orders_data:
        order_list = list(order)
        # Обработка created_at (индекс 12)
        if isinstance(order_list[12], str):
            try:
                from datetime import datetime
                order_list[12] = datetime.strptime(order_list[12], '%Y-%m-%d %H:%M:%S.%f')
            except:
                try:
                    order_list[12] = datetime.strptime(order_list[12], '%Y-%m-%d %H:%M:%S')
                except:
                    pass
        # Обработка updated_at (индекс 13)
        if isinstance(order_list[13], str):
            try:
                from datetime import datetime
                order_list[13] = datetime.strptime(order_list[13], '%Y-%m-%d %H:%M:%S.%f')
            except:
                try:
                    order_list[13] = datetime.strptime(order_list[13], '%Y-%m-%d %H:%M:%S')
                except:
                    pass
        processed_orders.append(tuple(order_list))
    
    cursor.close()
    conn.close()
    
    return render_template('orders.html', 
                         orders=processed_orders, 
                         stats=stats,
                         filter_status=status_filter)

@app.route('/update_order_status/<int:order_id>', methods=['POST'])
@manager_required
def update_order_status(order_id):
    new_status = request.form['status']
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Получаем старые данные
        cursor.execute("SELECT customer_name, status FROM orders WHERE id = %s", (order_id,))
        old_data = cursor.fetchone()
        customer_name = old_data[0]
        old_status = old_data[1]
        
        cursor.execute("""
            UPDATE orders 
            SET status = %s, updated_at = CURRENT_TIMESTAMP 
            WHERE id = %s
        """, (new_status, order_id))
        
        conn.commit()
        
        # Логируем изменение статуса
        log_action('update', 'order', order_id, customer_name,
                  f'Изменен статус заказа: {old_status} → {new_status}')
        
        flash('✅ Статус заказа обновлен!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'❌ Ошибка: {str(e)}', 'danger')
    finally:
        cursor.close()
        conn.close()
    
    return redirect(url_for('orders'))

@app.route('/update_order_notes/<int:order_id>', methods=['POST'])
@manager_required
def update_order_notes(order_id):
    notes = request.form['notes']
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT customer_name FROM orders WHERE id = %s", (order_id,))
        customer_name = cursor.fetchone()[0]
        
        cursor.execute("""
            UPDATE orders 
            SET notes = %s, updated_at = CURRENT_TIMESTAMP 
            WHERE id = %s
        """, (notes, order_id))
        
        conn.commit()
        
        # Логируем обновление примечаний
        log_action('update', 'order', order_id, customer_name,
                  f'Обновлены примечания к заказу')
        
        flash('✅ Примечания обновлены!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'❌ Ошибка: {str(e)}', 'danger')
    finally:
        cursor.close()
        conn.close()
    
    return redirect(url_for('orders'))

@app.route('/delete_order/<int:order_id>', methods=['POST'])
@manager_required
def delete_order(order_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT customer_name FROM orders WHERE id = %s", (order_id,))
        customer_name = cursor.fetchone()[0]
        
        cursor.execute("DELETE FROM orders WHERE id = %s", (order_id,))
        conn.commit()
        
        # Логируем удаление заказа
        log_action('delete', 'order', order_id, customer_name,
                  f'Удален заказ клиента: {customer_name}')
        
        flash('✅ Заказ успешно удален!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'❌ Ошибка при удалении: {str(e)}', 'danger')
    finally:
        cursor.close()
        conn.close()
    
    return redirect(url_for('orders'))

# ==================== РАЗДЕЛ ПОЛЬЗОВАТЕЛЕЙ ====================
@app.route('/users')
@admin_required
def users():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT id, username, role, email, phone, full_name, 
               salary, profile_photo, created_at, 
               COALESCE(full_name, username) as display_name
        FROM users 
        ORDER BY 
            CASE role
                WHEN 'admin' THEN 1
                WHEN 'manager' THEN 2
                ELSE 3
            END,
            created_at DESC
    """)
    users_data = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return render_template('users.html', users=users_data)

@app.route('/add_user', methods=['POST'])
@admin_required
def add_user():
    username = request.form['username']
    password = request.form['password']
    role = request.form['role']
    email = request.form.get('email')
    phone = request.form.get('phone')
    full_name = request.form.get('full_name')
    salary = request.form.get('salary') if role != 'admin' else 0
    
    # Валидация зарплаты
    if salary:
        salary_valid, salary_msg = validate_positive_number(salary, "Зарплата")
        if not salary_valid:
            flash(f'❌ {salary_msg}', 'danger')
            return redirect(url_for('users'))
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            INSERT INTO users (username, password, role, email, phone, full_name, salary)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (username, password, role, email, phone, full_name, salary))
        
        # Получаем ID добавленного пользователя
        cursor.execute("SELECT LASTVAL()")
        user_id = cursor.fetchone()[0]
        
        conn.commit()
        
        # Логируем создание пользователя
        log_action('create', 'user', user_id, username,
                  f'Создан пользователь: {username}, роль: {role}, ФИО: {full_name}')
        
        flash('✅ Пользователь успешно добавлен!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'❌ Ошибка при добавлении пользователя: {str(e)}', 'danger')
    finally:
        cursor.close()
        conn.close()
    
    return redirect(url_for('users'))

@app.route('/edit_user/<int:user_id>', methods=['POST'])
@admin_required
def edit_user(user_id):
    username = request.form['username']
    role = request.form['role']
    email = request.form.get('email')
    phone = request.form.get('phone')
    full_name = request.form.get('full_name')
    password = request.form.get('password')
    salary = request.form.get('salary') if role != 'admin' else 0
    
    # Валидация зарплаты
    if salary:
        salary_valid, salary_msg = validate_positive_number(salary, "Зарплата")
        if not salary_valid:
            flash(f'❌ {salary_msg}', 'danger')
            return redirect(url_for('users'))
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Получаем старые данные
        cursor.execute("SELECT username, role, full_name, salary FROM users WHERE id = %s", (user_id,))
        old_data = cursor.fetchone()
        old_username = old_data[0]
        old_role = old_data[1]
        old_full_name = old_data[2] or ""
        old_salary = old_data[3] or 0
        
        if password:
            cursor.execute("""
                UPDATE users 
                SET username=%s, role=%s, email=%s, phone=%s, full_name=%s, password=%s, salary=%s
                WHERE id=%s
            """, (username, role, email, phone, full_name, password, salary, user_id))
        else:
            cursor.execute("""
                UPDATE users 
                SET username=%s, role=%s, email=%s, phone=%s, full_name=%s, salary=%s
                WHERE id=%s
            """, (username, role, email, phone, full_name, salary, user_id))
        
        conn.commit()
        
        # Логируем изменения
        changes = []
        if username != old_username:
            changes.append(f"логин: {old_username} → {username}")
        if role != old_role:
            changes.append(f"роль: {old_role} → {role}")
        if full_name != old_full_name:
            changes.append(f"ФИО: {old_full_name} → {full_name}")
        if float(salary) != float(old_salary):
            changes.append(f"зарплата: {old_salary} → {salary}")
        
        if changes:
            log_action('update', 'user', user_id, username,
                      f'Изменен пользователь. Изменения: {", ".join(changes)}')
        
        flash('✅ Пользователь успешно обновлен!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'❌ Ошибка при обновлении пользователя: {str(e)}', 'danger')
    finally:
        cursor.close()
        conn.close()
    
    return redirect(url_for('users'))

@app.route('/delete_user/<int:user_id>', methods=['POST'])
@admin_required
def delete_user(user_id):
    if user_id == session['user_id']:
        flash('❌ Нельзя удалить свой собственный аккаунт!', 'danger')
        return redirect(url_for('users'))
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT username FROM users WHERE id = %s", (user_id,))
        username = cursor.fetchone()[0]
        
        cursor.execute("DELETE FROM users WHERE id = %s", (user_id,))
        conn.commit()
        
        # Логируем удаление пользователя
        log_action('delete', 'user', user_id, username,
                  f'Удален пользователь: {username}')
        
        flash('✅ Пользователь успешно удален!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'❌ Ошибка при удалении пользователя: {str(e)}', 'danger')
    finally:
        cursor.close()
        conn.close()
    
    return redirect(url_for('users'))

# ==================== РАЗДЕЛ ПРОФИЛЯ ====================
@app.route('/profile')
@worker_required
def profile():
    """Страница профиля пользователя"""
    user_id = session.get('user_id')
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT id, username, role, email, phone, full_name, 
               salary, profile_photo, created_at
        FROM users 
        WHERE id = %s
    """, (user_id,))
    
    user_data = cursor.fetchone()
    cursor.close()
    conn.close()
    
    if not user_data:
        flash('❌ Пользователь не найден!', 'danger')
        return redirect(url_for('index'))
    
    return render_template('profile.html', user=user_data)

@app.route('/update_profile', methods=['POST'])
@worker_required
def update_profile():
    """Обновление профиля пользователя"""
    user_id = session.get('user_id')
    username = request.form.get('username')
    full_name = request.form.get('full_name')
    email = request.form.get('email')
    phone = request.form.get('phone')
    password = request.form.get('password')
    confirm_password = request.form.get('confirm_password')
    
    # Валидация пароля
    if password:
        if len(password) < 6:
            flash('❌ Пароль должен содержать минимум 6 символов!', 'danger')
            return redirect(url_for('profile'))
        
        if password != confirm_password:
            flash('❌ Пароли не совпадают!', 'danger')
            return redirect(url_for('profile'))
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Получаем старые данные
        cursor.execute("SELECT username, full_name, email, phone FROM users WHERE id = %s", (user_id,))
        old_data = cursor.fetchone()
        old_username = old_data[0]
        old_full_name = old_data[1] or ""
        old_email = old_data[2] or ""
        old_phone = old_data[3] or ""
        
        # Проверяем уникальность username (если изменили)
        if username and username != old_username:
            cursor.execute("SELECT id FROM users WHERE username = %s AND id != %s", (username, user_id))
            if cursor.fetchone():
                flash('❌ Этот логин уже занят!', 'danger')
                return redirect(url_for('profile'))
        
        # Обработка фото профиля
        photo_filename = None
        if 'profile_photo' in request.files:
            file = request.files['profile_photo']
            if file and file.filename and allowed_file(file.filename):
                # Удаляем старое фото если есть
                cursor.execute("SELECT profile_photo FROM users WHERE id = %s", (user_id,))
                old_photo = cursor.fetchone()[0]
                if old_photo:
                    old_path = os.path.join(app.config['UPLOAD_FOLDER'], old_photo)
                    if os.path.exists(old_path):
                        os.remove(old_path)
                
                # Сохраняем новое фото
                filename = secure_filename(file.filename)
                unique_filename = f"profile_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{filename}"
                file.save(os.path.join(app.config['UPLOAD_FOLDER'], unique_filename))
                photo_filename = unique_filename
        
        # Обновляем данные
        if photo_filename:
            if password:
                cursor.execute("""
                    UPDATE users 
                    SET username = %s, full_name = %s, email = %s, phone = %s, 
                        profile_photo = %s, password = %s
                    WHERE id = %s
                """, (username, full_name, email, phone, photo_filename, password, user_id))
            else:
                cursor.execute("""
                    UPDATE users 
                    SET username = %s, full_name = %s, email = %s, phone = %s, 
                        profile_photo = %s
                    WHERE id = %s
                """, (username, full_name, email, phone, photo_filename, user_id))
        else:
            if password:
                cursor.execute("""
                    UPDATE users 
                    SET username = %s, full_name = %s, email = %s, phone = %s, 
                        password = %s
                    WHERE id = %s
                """, (username, full_name, email, phone, password, user_id))
            else:
                cursor.execute("""
                    UPDATE users 
                    SET username = %s, full_name = %s, email = %s, phone = %s
                    WHERE id = %s
                """, (username, full_name, email, phone, user_id))
        
        # Обновляем сессию
        cursor.execute("SELECT username, role, full_name FROM users WHERE id = %s", (user_id,))
        user = cursor.fetchone()
        session['username'] = user[0]
        session['role'] = user[1]
        session['full_name'] = user[2]
        
        conn.commit()
        
        # Логируем обновление профиля
        changes = []
        if username != old_username:
            changes.append(f"логин: {old_username} → {username}")
        if full_name != old_full_name:
            changes.append(f"ФИО: {old_full_name} → {full_name}")
        if email != old_email:
            changes.append(f"email: {old_email} → {email}")
        if phone != old_phone:
            changes.append(f"телефон: {old_phone} → {phone}")
        if password:
            changes.append("пароль изменен")
        
        if changes:
            log_action('update', 'profile', user_id, username,
                      f'Обновлен профиль. Изменения: {", ".join(changes)}')
        
        flash('✅ Профиль успешно обновлен!', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'❌ Ошибка при обновлении профиля: {str(e)}', 'danger')
    
    finally:
        cursor.close()
        conn.close()
    
    return redirect(url_for('profile'))

@app.route('/delete_profile_photo', methods=['POST'])
@worker_required
def delete_profile_photo():
    """Удаление фото профиля"""
    user_id = session.get('user_id')
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Получаем имя файла фото
        cursor.execute("SELECT username, profile_photo FROM users WHERE id = %s", (user_id,))
        user_data = cursor.fetchone()
        username = user_data[0]
        photo_filename = user_data[1]
        
        if photo_filename:
            # Удаляем файл с сервера
            photo_path = os.path.join(app.config['UPLOAD_FOLDER'], photo_filename)
            if os.path.exists(photo_path):
                os.remove(photo_path)
            
            # Обновляем базу данных
            cursor.execute("UPDATE users SET profile_photo = NULL WHERE id = %s", (user_id,))
            conn.commit()
            
            # Логируем удаление фото
            log_action('update', 'profile', user_id, username,
                      f'Удалено фото профиля')
            
            flash('✅ Фото профиля удалено!', 'success')
        else:
            flash('⚠️ Фото профиля не найдено', 'warning')
    
    except Exception as e:
        conn.rollback()
        flash(f'❌ Ошибка при удалении фото: {str(e)}', 'danger')
    
    finally:
        cursor.close()
        conn.close()
    
    return redirect(url_for('profile'))

# ==================== ИСТОРИЯ ИЗМЕНЕНИЙ ====================
@app.route('/logs')
@admin_required
def logs():
    page = request.args.get('page', 1, type=int)
    per_page = 50
    offset = (page - 1) * per_page
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Получаем общее количество записей
    cursor.execute("SELECT COUNT(*) FROM action_logs")
    total_logs = cursor.fetchone()[0]
    
    # Получаем логи с пагинацией
    cursor.execute("""
        SELECT 
            al.*, 
            u.full_name as user_full_name,
            u.role as user_role
        FROM action_logs al
        LEFT JOIN users u ON al.user_id = u.id
        ORDER BY al.created_at DESC
        LIMIT %s OFFSET %s
    """, (per_page, offset))
    
    logs_data = cursor.fetchall()
    
    # Получаем статистику по действиям
    cursor.execute("""
        SELECT 
            action_type,
            COUNT(*) as count
        FROM action_logs
        GROUP BY action_type
        ORDER BY count DESC
    """)
    action_stats = cursor.fetchall()
    
    # Получаем статистику по сущностям
    cursor.execute("""
        SELECT 
            entity_type,
            COUNT(*) as count
        FROM action_logs
        GROUP BY entity_type
        ORDER BY count DESC
    """)
    entity_stats = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    total_pages = (total_logs + per_page - 1) // per_page
    
    return render_template('logs.html',
                         logs=logs_data,
                         page=page,
                         total_pages=total_pages,
                         total_logs=total_logs,
                         action_stats=action_stats,
                         entity_stats=entity_stats,
                         per_page=per_page)

@app.route('/api/clear_logs', methods=['POST'])
@admin_required
def clear_logs():
    """Очистка всей истории изменений"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("DELETE FROM action_logs")
        conn.commit()
        
        # Логируем очистку логов
        log_action('delete', 'system', details='Очищена вся история изменений')
        
        cursor.close()
        conn.close()
        return jsonify({'success': True, 'message': 'История очищена'})
    except Exception as e:
        conn.rollback()
        cursor.close()
        conn.close()
        return jsonify({'success': False, 'error': str(e)}), 500
    
@app.route('/game')
def game():
    """Страница игры Ферма"""
    if 'user_id' not in session:
        flash('Войдите в систему, чтобы играть', 'warning')
        return redirect(url_for('login'))
    return render_template('game.html')

# ==================== РАЗШИРЕННЫЙ ЧАТ С ЛИЧНЫМИ СООБЩЕНИЯМИ ====================

@app.route('/get_available_chats')
@worker_required
def get_available_chats():
    """Получить список доступных чатов (общий + пользователи)"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Получаем список пользователей (кроме себя)
    cursor.execute("""
        SELECT id, username, full_name, role 
        FROM users 
        WHERE id != %s 
        ORDER BY full_name
    """, (session['user_id'],))
    
    users = cursor.fetchall()
    
    # Начинаем список с общего чата
    chats_list = [{
        'id': 'global',
        'name': 'Общий чат',
        'type': 'global',
        'unread': 0
    }]
    
    # Получаем количество непрочитанных в общем чате
    cursor.execute("""
        SELECT last_read_global_id 
        FROM users 
        WHERE id = %s
    """, (session['user_id'],))
    
    row = cursor.fetchone()
    last_read_id = row[0] if row and row[0] else 0
    
    cursor.execute("""
        SELECT COUNT(*) 
        FROM global_chat
        WHERE id > %s AND user_id != %s
    """, (last_read_id, session['user_id']))
    
    global_unread = cursor.fetchone()[0] or 0
    chats_list[0]['unread'] = global_unread
    
    for user in users:
        # Проверяем есть ли непрочитанные сообщения от этого пользователя
        cursor.execute("""
            SELECT COUNT(*) FROM private_messages 
            WHERE sender_id = %s AND receiver_id = %s AND is_read = FALSE
        """, (user[0], session['user_id']))
        
        unread_count = cursor.fetchone()[0] or 0
        
        chats_list.append({
            'id': user[0],
            'name': user[2] or user[1],
            'role': user[3],
            'type': 'private',
            'unread': unread_count
        })
    
    cursor.close()
    conn.close()
    
    return jsonify(chats_list)

@app.route('/send_chat_message', methods=['POST'])
@worker_required
def send_chat_message():
    """Отправить сообщение в общий чат"""
    try:
        data = request.json
        if not data:
            return jsonify({'success': False, 'error': 'Нет данных'})
        
        message = data.get('message', '').strip()
        if not message:
            return jsonify({'success': False, 'error': 'Сообщение не может быть пустым'})
        
        # Берем данные из сессии, а не из запроса (для безопасности)
        user_id = session.get('user_id')
        username = session.get('username')
        full_name = session.get('full_name')
        role = session.get('role')
        
        if not user_id:
            return jsonify({'success': False, 'error': 'Пользователь не авторизован'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO global_chat (user_id, username, full_name, role, message)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            """, (user_id, username, full_name, role, message))
            
            message_id = cursor.fetchone()[0]
            conn.commit()
            
            # Логируем отправку сообщения
            log_action('create', 'chat_message', message_id, f'Сообщение в общем чате', 
                      f'Отправлено сообщение: {message[:50]}...')
            
            return jsonify({'success': True, 'message_id': message_id})
            
        except Exception as e:
            conn.rollback()
            print(f"Ошибка при отправке сообщения в чат: {e}")
            return jsonify({'success': False, 'error': 'Ошибка базы данных'})
            
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        print(f"Ошибка в send_chat_message: {e}")
        return jsonify({'success': False, 'error': 'Ошибка сервера'}), 500
    
@app.route('/get_unread_count')
@worker_required
def get_unread_count():
    """Получить количество непрочитанных сообщений"""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        user_id = session['user_id']

        # Получаем ID последнего прочитанного сообщения
        cursor.execute("""
            SELECT last_read_global_id 
            FROM users 
            WHERE id = %s
        """, (user_id,))
        row = cursor.fetchone()
        last_read_id = row[0] if row and row[0] else 0

        # Считаем только сообщения новее
        cursor.execute("""
            SELECT COUNT(*) 
            FROM global_chat
            WHERE id > %s AND user_id != %s
        """, (last_read_id, user_id))
        global_unread = cursor.fetchone()[0] or 0

        # Непрочитанные личные сообщения
        cursor.execute("""
            SELECT COUNT(*) FROM private_messages 
            WHERE receiver_id = %s AND is_read = FALSE
        """, (user_id,))
        private_unread = cursor.fetchone()[0] or 0

        total_unread = global_unread + private_unread

        return jsonify({
            'global': global_unread,
            'private': private_unread,
            'total': total_unread
        })

    except Exception as e:
        return jsonify({'error': str(e)})
    finally:
        cursor.close()
        conn.close()

@app.route('/mark_global_as_read')
@worker_required
def mark_global_as_read():
    """Пометить сообщения общего чата как прочитанные"""
    # Для общего чата просто сбрасываем счетчик
    return jsonify({'success': True})

@app.route('/get_chat_messages')
@worker_required
def get_chat_messages():
    """Получить сообщения общего чата"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Берем последние 50 сообщений
        cursor.execute("""
            SELECT id, user_id, username, full_name, role, message, 
                   TO_CHAR(created_at, 'YYYY-MM-DD HH24:MI:SS') as created_at
            FROM global_chat
            ORDER BY created_at DESC
            LIMIT 50
        """)
        
        messages = cursor.fetchall()
        messages_list = []
        
        for msg in messages:
            messages_list.append({
                'id': msg[0],
                'user_id': msg[1],
                'username': msg[2],
                'full_name': msg[3],
                'role': msg[4],
                'message': msg[5],
                'created_at': msg[6]
            })
        
        cursor.close()
        conn.close()
        
        # Возвращаем в обратном порядке (новые внизу)
        messages_list.reverse()
        
        return jsonify(messages_list)
        
    except Exception as e:
        print(f"Ошибка при загрузке сообщений чата: {e}")
        return jsonify({'error': str(e)}), 500
    
@app.route('/send_private_message', methods=['POST'])
@worker_required
def send_private_message():
    """Отправить личное сообщение"""
    try:
        data = request.json
        if not data:
            return jsonify({'success': False, 'error': 'Нет данных'})
        
        receiver_id = data.get('receiver_id')
        message = data.get('message', '').strip()
        
        if not receiver_id:
            return jsonify({'success': False, 'error': 'Не указан получатель'})
        
        if not message:
            return jsonify({'success': False, 'error': 'Сообщение не может быть пустым'})
        
        sender_id = session.get('user_id')
        if not sender_id:
            return jsonify({'success': False, 'error': 'Пользователь не авторизован'})
        
        # Проверяем, что получатель существует
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT id FROM users WHERE id = %s", (receiver_id,))
            if not cursor.fetchone():
                return jsonify({'success': False, 'error': 'Получатель не найден'})
            
            # Вставляем сообщение
            cursor.execute("""
                INSERT INTO private_messages (sender_id, receiver_id, message)
                VALUES (%s, %s, %s)
                RETURNING id, created_at
            """, (sender_id, receiver_id, message))
            
            result = cursor.fetchone()
            message_id = result[0]
            created_at = result[1]
            
            conn.commit()
            
            # Получаем данные отправителя для ответа
            cursor.execute("""
                SELECT username, full_name, role 
                FROM users 
                WHERE id = %s
            """, (sender_id,))
            
            sender_data = cursor.fetchone()
            
            # Логируем отправку сообщения
            log_action('create', 'private_message', message_id, 
                      f'Личное сообщение пользователю {receiver_id}', 
                      f'Отправлено сообщение: {message[:50]}...')
            
            return jsonify({
                'success': True, 
                'message_id': message_id,
                'sender_id': sender_id,
                'sender_username': sender_data[0],
                'sender_full_name': sender_data[1],
                'sender_role': sender_data[2],
                'created_at': created_at.isoformat() if created_at else None
            })
            
        except Exception as e:
            conn.rollback()
            print(f"Ошибка при отправке личного сообщения: {e}")
            return jsonify({'success': False, 'error': 'Ошибка базы данных'})
            
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        print(f"Ошибка в send_private_message: {e}")
        return jsonify({'success': False, 'error': 'Ошибка сервера'}), 500
    
@app.route('/get_private_messages/<int:other_user_id>')
@worker_required
def get_private_messages_with_id(other_user_id):
    """Получить личные сообщения с конкретным пользователем"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Помечаем полученные сообщения как прочитанные
        cursor.execute("""
            UPDATE private_messages 
            SET is_read = TRUE 
            WHERE sender_id = %s AND receiver_id = %s AND is_read = FALSE
        """, (other_user_id, session['user_id']))
        
        # Получаем историю переписки
        cursor.execute("""
            SELECT 
                pm.id,
                pm.sender_id,
                pm.receiver_id,
                pm.message,
                pm.is_read,
                pm.created_at,
                sender.username as sender_username,
                sender.full_name as sender_full_name,
                sender.role as sender_role,
                receiver.username as receiver_username,
                receiver.full_name as receiver_full_name
            FROM private_messages pm
            LEFT JOIN users sender ON pm.sender_id = sender.id
            LEFT JOIN users receiver ON pm.receiver_id = receiver.id
            WHERE (pm.sender_id = %s AND pm.receiver_id = %s) 
               OR (pm.sender_id = %s AND pm.receiver_id = %s)
            ORDER BY pm.created_at DESC
            LIMIT 50
        """, (session['user_id'], other_user_id, other_user_id, session['user_id']))
        
        messages = cursor.fetchall()
        messages_list = []
        
        for msg in messages:
            messages_list.append({
                'id': msg[0],
                'sender_id': msg[1],
                'receiver_id': msg[2],
                'message': msg[3],
                'is_read': msg[4],
                'created_at': msg[5].isoformat() if msg[5] else None,
                'sender_username': msg[6],
                'sender_full_name': msg[7],
                'sender_role': msg[8],
                'receiver_username': msg[9],
                'receiver_full_name': msg[10]
            })
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify(messages_list)
        
    except Exception as e:
        print(f"Ошибка при загрузке личных сообщений: {e}")
        return jsonify({'error': str(e)}), 500
    
# ==================== ОЧИСТКА ЧАТОВ ====================

@app.route('/clear_chat', methods=['POST'])
@worker_required
def clear_chat():
    """Очистить чат"""
    data = request.json
    chat_type = data.get('chat_type')
    other_user_id = data.get('other_user_id')
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        if chat_type == 'global':
            # Проверяем права для очистки общего чата
            if session['role'] not in ['admin', 'manager']:
                return jsonify({'success': False, 'error': 'Только администратор или менеджер может очистить общий чат'})
            
            cursor.execute("DELETE FROM global_chat")
            flash('✅ Общий чат очищен!', 'success')
            
        elif chat_type == 'private' and other_user_id:
            # Очищаем личную переписку между двумя пользователями
            cursor.execute("""
                DELETE FROM private_messages 
                WHERE (sender_id = %s AND receiver_id = %s) 
                   OR (sender_id = %s AND receiver_id = %s)
            """, (session['user_id'], other_user_id, other_user_id, session['user_id']))
            flash('✅ Личная переписка очищена!', 'success')
        
        else:
            return jsonify({'success': False, 'error': 'Неверные параметры'})
        
        conn.commit()
        return jsonify({'success': True})
        
    except Exception as e:
        conn.rollback()
        return jsonify({'success': False, 'error': str(e)})
    finally:
        cursor.close()
        conn.close()

@app.route('/clear_my_chats', methods=['POST'])
@worker_required
def clear_my_chats():
    """Очистить все мои чаты"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Очищаем личные сообщения, где пользователь является отправителем или получателем
        cursor.execute("""
            DELETE FROM private_messages 
            WHERE sender_id = %s OR receiver_id = %s
        """, (session['user_id'], session['user_id']))
        
        conn.commit()
        return jsonify({'success': True})
        
    except Exception as e:
        conn.rollback()
        return jsonify({'success': False, 'error': str(e)})
    finally:
        cursor.close()
        conn.close()

# ==================== ИМПОРТ/ЭКСПОРТ EXCEL И CSV ====================
import pandas as pd
import io
import csv
from flask import send_file, Response

@app.route('/export/<table_name>/<format_type>')
@manager_required
def export_table(table_name, format_type):
    """Экспорт таблицы в Excel или CSV"""
    try:
        conn = get_db_connection()
        
        # Определяем таблицы и их SQL запросы
        table_queries = {
            'animals': """
                SELECT id, name, species, breed, birth_date, current_weight, 
                       status, vaccination_type, vaccination_date, 
                       next_vaccination_date, price
                FROM animals
                ORDER BY id
            """,
            'meat': """
                SELECT id, breed, birth_date, slaughter_date, carcass_weight, 
                       price, status, description
                FROM meat
                ORDER BY id
            """,
            'fields': """
                SELECT id, name, area, crop, last_seeding_date, 
                       expected_harvest_date, status, notes
                FROM fields
                ORDER BY id
            """,
            'storage': """
                SELECT id, product_type, feed_category, quantity, unit, 
                       min_quantity, price_per_unit
                FROM feed_types
                ORDER BY id
            """,
            'machinery': """
                SELECT id, type, model, serial_number, purchase_date, condition, 
                       last_service_date, next_service_date, service_notes
                FROM machinery
                ORDER BY id
            """,
            'finance': """
                SELECT id, type, category, amount, date, description, user_id
                FROM finance
                ORDER BY date DESC
            """,
            'users': """
                SELECT id, username, role, email, phone, full_name, salary, registered_at
                FROM users
                ORDER BY id
            """,
            'orders': """
                SELECT id, customer_name, phone, telegram, order_type, 
                       product_name, quantity, unit_price, total_amount, 
                       status, notes, created_at, updated_at
                FROM orders
                ORDER BY created_at DESC
            """
        }
        
        if table_name not in table_queries:
            flash('Таблица не найдена', 'danger')
            return redirect(url_for('dashboard'))
        
        # Получаем данные
        df = pd.read_sql_query(table_queries[table_name], conn)
        conn.close()
        
        # Русские названия столбцов для разных таблиц
        column_names = {
            'animals': {
                'id': 'ID',
                'name': 'Имя',
                'species': 'Вид',
                'breed': 'Порода',
                'birth_date': 'Дата рождения',
                'current_weight': 'Вес (кг)',
                'status': 'Статус',
                'vaccination_type': 'Тип вакцинации',
                'vaccination_date': 'Дата вакцинации',
                'next_vaccination_date': 'Следующая вакцинация',
                'price': 'Цена (₸)'
            },
            'meat': {
                'id': 'ID',
                'breed': 'Порода',
                'birth_date': 'Дата рождения',
                'slaughter_date': 'Дата забоя',
                'carcass_weight': 'Вес туши (кг)',
                'price': 'Цена (₸)',
                'status': 'Статус',
                'description': 'Описание'
            },
            'fields': {
                'id': 'ID',
                'name': 'Название',
                'area': 'Площадь (га)',
                'crop': 'Культура',
                'last_seeding_date': 'Дата посева',
                'expected_harvest_date': 'Дата уборки',
                'status': 'Статус',
                'notes': 'Примечания'
            },
            'storage': {
                'id': 'ID',
                'product_type': 'Тип корма',
                'feed_category': 'Категория',
                'quantity': 'Количество',
                'unit': 'Единица измерения',
                'min_quantity': 'Мин. запас',
                'price_per_unit': 'Цена за единицу'
            },
            'machinery': {
                'id': 'ID',
                'type': 'Тип техники',
                'model': 'Модель',
                'serial_number': 'Серийный номер',
                'purchase_date': 'Дата покупки',
                'condition': 'Состояние',
                'last_service_date': 'Дата последнего ТО',
                'next_service_date': 'Дата следующего ТО',
                'service_notes': 'Примечания'
            },
            'finance': {
                'id': 'ID',
                'type': 'Тип операции',
                'category': 'Категория',
                'amount': 'Сумма (₸)',
                'date': 'Дата',
                'description': 'Описание',
                'user_id': 'ID пользователя'
            },
            'users': {
                'id': 'ID',
                'username': 'Логин',
                'role': 'Роль',
                'email': 'Email',
                'phone': 'Телефон',
                'full_name': 'ФИО',
                'salary': 'Зарплата (₸)',
                'registered_at': 'Дата регистрации'
            },
            'orders': {
                'id': 'ID',
                'customer_name': 'Клиент',
                'phone': 'Телефон',
                'telegram': 'Telegram',
                'order_type': 'Тип заказа',
                'product_name': 'Товар',
                'quantity': 'Количество',
                'unit_price': 'Цена за единицу (₸)',
                'total_amount': 'Итого (₸)',
                'status': 'Статус',
                'notes': 'Примечания',
                'created_at': 'Дата создания',
                'updated_at': 'Дата обновления'
            }
        }
        
        # Переименовываем столбцы
        if table_name in column_names:
            df.rename(columns=column_names[table_name], inplace=True)
        
        # Форматируем даты
        date_columns = [col for col in df.columns if 'дата' in col.lower() or 'created_at' in col or 'updated_at' in col]
        for col in date_columns:
            if col in df.columns:
                try:
                    df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d %H:%M:%S')
                except:
                    pass
        
        filename = f"{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        if format_type == 'excel':
            # Экспорт в Excel
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name=table_name, index=False)
                # Настройка ширины столбцов
                worksheet = writer.sheets[table_name]
                for i, col in enumerate(df.columns):
                    column_width = max(df[col].astype(str).map(len).max(), len(col)) + 2
                    worksheet.column_dimensions[chr(65 + i)].width = min(column_width, 50)
            
            output.seek(0)
            
            # Логирование
            log_action('export', table_name, details=f'Экспорт в Excel: {table_name}')
            
            return send_file(
                output,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name=f'{filename}.xlsx'
            )
        
        elif format_type == 'csv':
            # Экспорт в CSV
            output = io.StringIO()
            df.to_csv(output, index=False, sep=';', encoding='utf-8-sig')
            
            # Логирование
            log_action('export', table_name, details=f'Экспорт в CSV: {table_name}')
            
            return Response(
                output.getvalue(),
                mimetype='text/csv',
                headers={
                    'Content-Disposition': f'attachment; filename={filename}.csv',
                    'Content-Type': 'text/csv; charset=utf-8-sig'
                }
            )
        
    except Exception as e:
        flash(f'Ошибка экспорта: {str(e)}', 'danger')
        return redirect(request.referrer or url_for('dashboard'))

@app.route('/import/<table_name>', methods=['POST'])
@manager_required
def import_table(table_name):
    """Импорт данных из Excel или CSV"""
    try:
        if 'file' not in request.files:
            flash('Файл не выбран', 'danger')
            return redirect(request.referrer)
        
        file = request.files['file']
        if file.filename == '':
            flash('Файл не выбран', 'danger')
            return redirect(request.referrer)
        
        file_extension = file.filename.split('.')[-1].lower()
        
        # Определяем правила валидации для каждой таблицы
        validation_rules = {
            'animals': {
                'required': ['name', 'species', 'breed', 'current_weight', 'status'],
                'numeric': ['current_weight', 'price'],
                'date': ['birth_date', 'vaccination_date', 'next_vaccination_date']
            },
            'meat': {
                'required': ['breed', 'carcass_weight', 'price', 'status'],
                'numeric': ['carcass_weight', 'price'],
                'date': ['birth_date', 'slaughter_date']
            },
            'fields': {
                'required': ['name', 'area', 'crop', 'status'],
                'numeric': ['area'],
                'date': ['last_seeding_date', 'expected_harvest_date']
            },
            'storage': {
                'required': ['product_type', 'feed_category', 'quantity', 'unit'],
                'numeric': ['quantity', 'min_quantity', 'price_per_unit']
            },
            'machinery': {
                'required': ['type', 'model', 'condition'],
                'numeric': [],
                'date': ['purchase_date', 'last_service_date', 'next_service_date']
            },
            'finance': {
                'required': ['type', 'category', 'amount', 'date'],
                'numeric': ['amount'],
                'date': ['date']
            },
            'users': {
                'required': ['username', 'role'],
                'numeric': ['salary'],
                'date': ['registered_at']
            },
            'orders': {
                'required': ['customer_name', 'order_type', 'quantity', 'unit_price'],
                'numeric': ['quantity', 'unit_price', 'total_amount'],
                'date': ['created_at', 'updated_at']
            }
        }
        
        # Определяем соответствие столбцов для импорта
        column_mapping = {
            'animals': {
                'ID': 'id',
                'Имя': 'name',
                'Вид': 'species',
                'Порода': 'breed',
                'Дата рождения': 'birth_date',
                'Вес (кг)': 'current_weight',
                'Статус': 'status',
                'Тип вакцинации': 'vaccination_type',
                'Дата вакцинации': 'vaccination_date',
                'Следующая вакцинация': 'next_vaccination_date',
                'Цена (₸)': 'price'
            },
            'meat': {
                'ID': 'id',
                'Порода': 'breed',
                'Дата рождения': 'birth_date',
                'Дата забоя': 'slaughter_date',
                'Вес туши (кг)': 'carcass_weight',
                'Цена (₸)': 'price',
                'Статус': 'status',
                'Описание': 'description'
            },
            'fields': {
                'ID': 'id',
                'Название': 'name',
                'Площадь (га)': 'area',
                'Культура': 'crop',
                'Дата посева': 'last_seeding_date',
                'Дата уборки': 'expected_harvest_date',
                'Статус': 'status',
                'Примечания': 'notes'
            },
            'storage': {
                'ID': 'id',
                'Тип корма': 'product_type',
                'Категория': 'feed_category',
                'Количество': 'quantity',
                'Единица измерения': 'unit',
                'Мин. запас': 'min_quantity',
                'Цена за единицу': 'price_per_unit'
            },
            'machinery': {
                'ID': 'id',
                'Тип техники': 'type',
                'Модель': 'model',
                'Серийный номер': 'serial_number',
                'Дата покупки': 'purchase_date',
                'Состояние': 'condition',
                'Дата последнего ТО': 'last_service_date',
                'Дата следующего ТО': 'next_service_date',
                'Примечания': 'service_notes'
            },
            'finance': {
                'ID': 'id',
                'Тип операции': 'type',
                'Категория': 'category',
                'Сумма (₸)': 'amount',
                'Дата': 'date',
                'Описание': 'description',
                'ID пользователя': 'user_id'
            },
            'users': {
                'ID': 'id',
                'Логин': 'username',
                'Роль': 'role',
                'Email': 'email',
                'Телефон': 'phone',
                'ФИО': 'full_name',
                'Зарплата (₸)': 'salary',
                'Дата регистрации': 'registered_at'
            },
            'orders': {
                'ID': 'id',
                'Клиент': 'customer_name',
                'Телефон': 'phone',
                'Telegram': 'telegram',
                'Тип заказа': 'order_type',
                'Товар': 'product_name',
                'Количество': 'quantity',
                'Цена за единицу (₸)': 'unit_price',
                'Итого (₸)': 'total_amount',
                'Статус': 'status',
                'Примечания': 'notes',
                'Дата создания': 'created_at',
                'Дата обновления': 'updated_at'
            }
        }
        
        # Чтение файла
        if file_extension in ['xlsx', 'xls']:
            df = pd.read_excel(file)
        elif file_extension == 'csv':
            try:
                df = pd.read_csv(file, sep=';', encoding='utf-8-sig')
            except:
                df = pd.read_csv(file, encoding='utf-8')
        else:
            flash('Неподдерживаемый формат файла. Используйте Excel (.xlsx, .xls) или CSV (.csv)', 'danger')
            return redirect(request.referrer)
        
        # Переименовываем столбцы согласно маппингу
        if table_name in column_mapping:
            df.rename(columns={k: v for k, v in column_mapping[table_name].items() if k in df.columns}, inplace=True)
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        imported_count = 0
        errors = []
        
        # Обрабатываем каждую строку
        for index, row in df.iterrows():
            try:
                # Преобразуем строку в словарь
                row_dict = row.where(pd.notnull(row), None).to_dict()
                
                # Валидация обязательных полей
                if table_name in validation_rules:
                    rules = validation_rules[table_name]
                    missing_fields = []
                    
                    for field in rules['required']:
                        if field not in row_dict or row_dict[field] is None or str(row_dict[field]).strip() == '':
                            missing_fields.append(field)
                    
                    if missing_fields:
                        errors.append(f"Строка {index + 2}: Отсутствуют обязательные поля: {', '.join(missing_fields)}")
                        continue
                
                # Определяем SQL запросы для вставки
                insert_queries = {
                    'animals': """
                        INSERT INTO animals (name, species, breed, birth_date, current_weight, status, 
                                            vaccination_type, vaccination_date, next_vaccination_date, price)
                        VALUES (%(name)s, %(species)s, %(breed)s, %(birth_date)s, %(current_weight)s, 
                                %(status)s, %(vaccination_type)s, %(vaccination_date)s, %(next_vaccination_date)s, %(price)s)
                    """,
                    'meat': """
                        INSERT INTO meat (breed, birth_date, slaughter_date, carcass_weight, price, status, description)
                        VALUES (%(breed)s, %(birth_date)s, %(slaughter_date)s, %(carcass_weight)s, 
                                %(price)s, %(status)s, %(description)s)
                    """,
                    'fields': """
                        INSERT INTO fields (name, area, crop, last_seeding_date, expected_harvest_date, status, notes)
                        VALUES (%(name)s, %(area)s, %(crop)s, %(last_seeding_date)s, %(expected_harvest_date)s, 
                                %(status)s, %(notes)s)
                    """,
                    'storage': """
                        INSERT INTO feed_types (product_type, feed_category, quantity, unit, min_quantity, price_per_unit)
                        VALUES (%(product_type)s, %(feed_category)s, %(quantity)s, %(unit)s, 
                                %(min_quantity)s, %(price_per_unit)s)
                    """,
                    'machinery': """
                        INSERT INTO machinery (type, model, serial_number, purchase_date, condition, 
                                              last_service_date, next_service_date, service_notes)
                        VALUES (%(type)s, %(model)s, %(serial_number)s, %(purchase_date)s, %(condition)s, 
                                %(last_service_date)s, %(next_service_date)s, %(service_notes)s)
                    """,
                    'finance': """
                        INSERT INTO finance (type, category, amount, date, description, user_id)
                        VALUES (%(type)s, %(category)s, %(amount)s, %(date)s, %(description)s, %(user_id)s)
                    """,
                    'users': """
                        INSERT INTO users (username, role, email, phone, full_name, salary, registered_at)
                        VALUES (%(username)s, %(role)s, %(email)s, %(phone)s, %(full_name)s, %(salary)s, %(registered_at)s)
                    """,
                    'orders': """
                        INSERT INTO orders (customer_name, phone, telegram, order_type, product_name, 
                                           quantity, unit_price, total_amount, status, notes)
                        VALUES (%(customer_name)s, %(phone)s, %(telegram)s, %(order_type)s, %(product_name)s, 
                                %(quantity)s, %(unit_price)s, %(total_amount)s, %(status)s, %(notes)s)
                    """
                }
                
                if table_name in insert_queries:
                    cursor.execute(insert_queries[table_name], row_dict)
                    imported_count += 1
                
            except Exception as e:
                errors.append(f"Строка {index + 2}: {str(e)}")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # Логирование
        log_action('import', table_name, details=f'Импорт {imported_count} записей в {table_name}')
        
        if errors:
            flash(f'Успешно импортировано {imported_count} записей. Ошибки: {len(errors)}', 'warning')
            # Сохраняем ошибки в сессии для отображения
            session['import_errors'] = errors[:10]  # Ограничиваем показ 10 ошибками
        else:
            flash(f'✅ Успешно импортировано {imported_count} записей', 'success')
        
        return redirect(request.referrer)
        
    except Exception as e:
        flash(f'Ошибка импорта: {str(e)}', 'danger')
        return redirect(request.referrer)

@app.route('/export_all/excel')
@manager_required
def export_all_excel():
    """Экспорт всех таблиц в один Excel файл с разными листами"""
    try:
        conn = get_db_connection()
        output = io.BytesIO()
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            tables = [
                ('animals', 'Животные'),
                ('meat', 'Мясо'),
                ('fields', 'Поля'),
                ('storage', 'Склад'),
                ('machinery', 'Техника'),
                ('finance', 'Финансы'),
                ('users', 'Сотрудники'),
                ('orders', 'Заказы')
            ]
            
            for table_name, sheet_name in tables:
                try:
                    df = pd.read_sql_query(f"SELECT * FROM {table_name} ORDER BY id", conn)
                    
                    # Форматируем даты
                    for col in df.select_dtypes(include=['datetime64']).columns:
                        df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
                    
                    # Записываем в Excel
                    df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
                    
                    # Настройка ширины столбцов
                    worksheet = writer.sheets[sheet_name[:31]]
                    for i, col in enumerate(df.columns):
                        column_width = max(df[col].astype(str).map(len).max(), len(col)) + 2
                        worksheet.column_dimensions[chr(65 + i)].width = min(column_width, 50)
                        
                except Exception as e:
                    print(f"Ошибка при экспорте таблицы {table_name}: {str(e)}")
        
        output.seek(0)
        filename = f"agro_farm_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        # Логирование
        log_action('export', 'all_tables', details='Экспорт всех таблиц в Excel')
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        flash(f'Ошибка экспорта всех таблиц: {str(e)}', 'danger')
        return redirect(url_for('dashboard'))
    
    
# ==================== ВЕРХНЯЯ ПАНЕЛЬ С ПОГОДОЙ И ПОИСКОМ ====================
import requests
from datetime import datetime

# Конфигурация OpenWeather API (получите ключ на https://openweathermap.org/api)
OPENWEATHER_API_KEY = 'ваш_api_ключ_здесь'  # Замените на ваш ключ

@app.route('/api/weather')
@worker_required
def get_weather():
    """Получение данных о погоде"""
    try:
        # Для примепа берем фиксированный город, можно сделать настраиваемым
        city = "Астана"  # Можно заменить на город из настроек пользователя
        
        url = f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={OPENWEATHER_API_KEY}&units=metric&lang=ru'
        response = requests.get(url, timeout=5)
        
        if response.status_code == 200:
            data = response.json()
            weather_data = {
                'city': data['name'],
                'temperature': round(data['main']['temp']),
                'description': data['weather'][0]['description'],
                'humidity': data['main']['humidity'],
                'wind_speed': data['wind']['speed'],
                'icon': data['weather'][0]['icon'],
                'feels_like': round(data['main']['feels_like'])
            }
            return jsonify({'success': True, 'weather': weather_data})
        else:
            return jsonify({'success': False, 'error': 'Не удалось получить данные о погоде'})
            
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/search')
@worker_required
def search():
    query = request.args.get('q', '').strip()
    if not query or len(query) < 2:
        return jsonify({'results': []})
    
    conn = get_db_connection()
    cursor = conn.cursor()
    results = []
    
    try:
        # Поиск по животным с расширенной информацией
        cursor.execute("""
            SELECT id, name, species, breed, status, current_weight,
                   'animal' as type, 'Животное' as type_name,
                   photo, created_at, price
            FROM animals 
            WHERE name ILIKE %s OR breed ILIKE %s OR species ILIKE %s
               OR id::text ILIKE %s
            ORDER BY 
                CASE 
                    WHEN name ILIKE %s THEN 1
                    WHEN breed ILIKE %s THEN 2
                    ELSE 3
                END
            LIMIT 10
        """, (f'%{query}%', f'%{query}%', f'%{query}%', f'%{query}%',
              f'%{query}%', f'%{query}%'))
        
        for row in cursor.fetchall():
            results.append({
                'id': row[0],
                'name': row[1],
                'details': f"{row[2]} • {row[3]} • Вес: {row[5]}кг",
                'status': row[4],
                'type': row[6],
                'type_name': row[7],
                'photo': row[8],
                'created_at': row[9].strftime('%Y-%m-%d') if row[9] else '',
                'price': row[10],
                'highlight_url': f'/animals?highlight={row[0]}'
            })
        
        # Поиск по задачам
        cursor.execute("""
            SELECT id, title, description, status, priority, due_date,
                   'task' as type, 'Задача' as type_name
            FROM tasks 
            WHERE title ILIKE %s OR description ILIKE %s OR id::text ILIKE %s
            ORDER BY 
                CASE 
                    WHEN title ILIKE %s THEN 1
                    ELSE 2
                END,
                CASE priority
                    WHEN 'высокий' THEN 1
                    WHEN 'средний' THEN 2
                    WHEN 'низкий' THEN 3
                END
            LIMIT 10
        """, (f'%{query}%', f'%{query}%', f'%{query}%', f'%{query}%'))
        
        for row in cursor.fetchall():
            results.append({
                'id': row[0],
                'name': row[1],
                'details': f"{row[2][:50]}{'...' if len(row[2]) > 50 else ''} • Приоритет: {row[4]}",
                'status': row[3],
                'due_date': row[5].strftime('%Y-%m-%d') if row[5] else '',
                'type': row[6],
                'type_name': row[7],
                'highlight_url': f'/tasks?highlight={row[0]}'
            })
        
        # Поиск по полям
        cursor.execute("""
            SELECT id, name, crop, status, area, expected_harvest_date,
                   'field' as type, 'Поле' as type_name
            FROM fields 
            WHERE name ILIKE %s OR crop ILIKE %s OR id::text ILIKE %s
            LIMIT 10
        """, (f'%{query}%', f'%{query}%', f'%{query}%'))
        
        for row in cursor.fetchall():
            results.append({
                'id': row[0],
                'name': row[1],
                'details': f"{row[2]} • {row[3]}га • Статус: {row[3]}",
                'status': row[3],
                'type': row[6],
                'type_name': row[7],
                'highlight_url': f'/fields?highlight={row[0]}'
            })
        
        # Поиск по сотрудникам
        if session.get('role') in ['admin', 'manager']:
            cursor.execute("""
                SELECT id, username, full_name, role, phone,
                       'user' as type, 'Сотрудник' as type_name
                FROM users 
                WHERE username ILIKE %s OR full_name ILIKE %s 
                   OR phone ILIKE %s OR id::text ILIKE %s
                LIMIT 5
            """, (f'%{query}%', f'%{query}%', f'%{query}%', f'%{query}%'))
            
            for row in cursor.fetchall():
                results.append({
                    'id': row[0],
                    'name': row[1],
                    'details': f"{row[2]} • Роль: {row[3]}",
                    'status': row[3],
                    'type': row[5],
                    'type_name': row[6],
                    'highlight_url': f'/users?highlight={row[0]}'
                })
        
        cursor.close()
        conn.close()
        
        return jsonify({'results': results, 'query': query})
        
    except Exception as e:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        return jsonify({'results': [], 'error': str(e)})
    
# ==================== СИСТЕМА ЗАПРОСОВ НА ОБЩЕНИЕ ====================

@app.route('/request_chat_permission', methods=['POST'])
@worker_required
def request_chat_permission():
    """Отправка запроса на общение с администратором"""
    try:
        data = request.json
        if not data:
            return jsonify({'success': False, 'error': 'Нет данных'})
        
        admin_id = data.get('admin_id')
        message = data.get('message', '').strip()
        
        if not admin_id:
            return jsonify({'success': False, 'error': 'Не выбран администратор'})
        
        if not message:
            return jsonify({'success': False, 'error': 'Введите причину для общения'})
        
        # Проверяем, что выбранный пользователь действительно админ
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT role FROM users WHERE id = %s", (admin_id,))
        user = cursor.fetchone()
        
        if not user or user[0] != 'admin':
            return jsonify({'success': False, 'error': 'Выбранный пользователь не является администратором'})
        
        # Проверяем, не отправлен ли уже запрос
        cursor.execute("""
            SELECT id FROM chat_requests 
            WHERE user_id = %s AND admin_id = %s AND status = 'pending'
        """, (session['user_id'], admin_id))
        
        if cursor.fetchone():
            return jsonify({'success': False, 'error': 'Запрос уже отправлен. Ожидайте ответа.'})
        
        # Создаем запрос
        cursor.execute("""
            INSERT INTO chat_requests (user_id, admin_id, message, status)
            VALUES (%s, %s, %s, 'pending')
        """, (session['user_id'], admin_id, message))
        
        # Получаем ID созданного запроса
        cursor.execute("SELECT LASTVAL()")
        request_id = cursor.fetchone()[0]
        
        conn.commit()
        
        # Логируем отправку запроса
        log_action('create', 'chat_request', request_id, 
                  f'Запрос на общение с администратором ID:{admin_id}',
                  f'Сообщение: {message[:100]}...')
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'request_id': request_id,
            'message': '✅ Запрос на общение отправлен! Администратор рассмотрит его в ближайшее время.'
        })
        
    except Exception as e:
        print(f"Ошибка в request_chat_permission: {e}")
        return jsonify({'success': False, 'error': 'Ошибка сервера'}), 500

@app.route('/get_chat_requests')
@admin_required
def get_chat_requests():
    """Получение списка запросов на общение (только для админа)"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Получаем активные запросы
        cursor.execute("""
            SELECT 
                cr.id,
                cr.user_id,
                cr.message,
                cr.status,
                cr.created_at,
                cr.responded_at,
                cr.expires_at,
                u.username,
                u.full_name,
                u.role
            FROM chat_requests cr
            JOIN users u ON cr.user_id = u.id
            WHERE cr.admin_id = %s AND cr.status != 'deleted'
            ORDER BY 
                CASE WHEN cr.status = 'pending' THEN 1
                     WHEN cr.status = 'approved' THEN 2
                     ELSE 3
                END,
                cr.created_at DESC
        """, (session['user_id'],))
        
        requests = cursor.fetchall()
        
        requests_list = []
        for req in requests:
            requests_list.append({
                'id': req[0],
                'user_id': req[1],
                'message': req[2],
                'status': req[3],
                'created_at': req[4].isoformat() if req[4] else None,
                'responded_at': req[5].isoformat() if req[5] else None,
                'expires_at': req[6].isoformat() if req[6] else None,
                'username': req[7],
                'full_name': req[8],
                'role': req[9]
            })
        
        cursor.close()
        conn.close()
        
        return jsonify(requests_list)
        
    except Exception as e:
        print(f"Ошибка в get_chat_requests: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/respond_chat_request', methods=['POST'])
@admin_required
def respond_chat_request():
    """Ответ на запрос общения (разрешить/отклонить)"""
    try:
        data = request.json
        if not data:
            return jsonify({'success': False, 'error': 'Нет данных'})
        
        request_id = data.get('request_id')
        response = data.get('response')  # 'approved', 'rejected', или 'deleted'
        expires_in = data.get('expires_in')  # '10min', '30min', '1hour', 'permanent'
        
        if not request_id or response not in ['approved', 'rejected', 'deleted']:
            return jsonify({'success': False, 'error': 'Некорректные данные'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Получаем информацию о запросе
        cursor.execute("""
            SELECT user_id, admin_id, message 
            FROM chat_requests 
            WHERE id = %s AND admin_id = %s
        """, (request_id, session['user_id']))
        
        request_data = cursor.fetchone()
        if not request_data:
            return jsonify({'success': False, 'error': 'Запрос не найден'})
        
        user_id, admin_id, message = request_data
        
        # Рассчитываем время истечения разрешения
        expires_at = None
        if response == 'approved' and expires_in:
            if expires_in == '10min':
                expires_at = datetime.now() + timedelta(minutes=10)
            elif expires_in == '30min':
                expires_at = datetime.now() + timedelta(minutes=30)
            elif expires_in == '1hour':
                expires_at = datetime.now() + timedelta(hours=1)
            elif expires_in == 'permanent':
                expires_at = None  # Бессрочное разрешение
            elif expires_in == 'custom':
                custom_minutes = data.get('custom_minutes')
                if custom_minutes:
                    expires_at = datetime.now() + timedelta(minutes=int(custom_minutes))
        
        # Обновляем запрос
        cursor.execute("""
            UPDATE chat_requests 
            SET status = %s, responded_at = CURRENT_TIMESTAMP, expires_at = %s 
            WHERE id = %s
        """, (response, expires_at, request_id))
        
        # Получаем данные пользователя для лога
        cursor.execute("SELECT username, full_name FROM users WHERE id = %s", (user_id,))
        user_data = cursor.fetchone()
        username = user_data[0] if user_data else "Неизвестно"
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # Логируем ответ
        duration_text = "бессрочно" if not expires_at else expires_in
        log_action('update', 'chat_request', request_id, 
                  f'Ответ на запрос от {username}',
                  f'Статус: {response}, срок: {duration_text}, пользователь: {username}')
        
        return jsonify({
            'success': True,
            'message': f'✅ Запрос {"одобрен" if response == "approved" else "отклонен"}!'
        })
        
    except Exception as e:
        print(f"Ошибка в respond_chat_request: {e}")
        return jsonify({'success': False, 'error': 'Ошибка сервера'}), 500

@app.route('/check_chat_permission/<int:admin_id>')
@worker_required
def check_chat_permission(admin_id):
    """Проверка разрешения на общение с администратором"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT status, created_at, expires_at 
            FROM chat_requests 
            WHERE user_id = %s AND admin_id = %s AND status = 'approved'
            ORDER BY created_at DESC 
            LIMIT 1
        """, (session['user_id'], admin_id))
        
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if result:
            status, created_at, expires_at = result
            
            # Проверяем, не истекло ли разрешение
            if expires_at and datetime.now() > expires_at:
                return jsonify({
                    'has_permission': False,
                    'status': 'expired',
                    'message': 'Срок разрешения истёк'
                })
            
            return jsonify({
                'has_permission': True,
                'status': status,
                'granted_at': created_at.isoformat() if created_at else None,
                'expires_at': expires_at.isoformat() if expires_at else None
            })
        else:
            return jsonify({
                'has_permission': False,
                'status': 'none',
                'message': 'Запрос не отправлен'
            })
            
    except Exception as e:
        print(f"Ошибка в check_chat_permission: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/delete_chat_request/<int:request_id>', methods=['POST'])
@worker_required
def delete_chat_request(request_id):
    """Удаление запроса пользователем"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Проверяем, что запрос принадлежит пользователю
        cursor.execute("""
            SELECT user_id, status FROM chat_requests 
            WHERE id = %s AND user_id = %s
        """, (request_id, session['user_id']))
        
        request_data = cursor.fetchone()
        if not request_data:
            return jsonify({'success': False, 'error': 'Запрос не найден'})
        
        user_id, status = request_data
        
        # Обновляем статус на deleted
        cursor.execute("""
            UPDATE chat_requests 
            SET status = 'deleted', responded_at = CURRENT_TIMESTAMP 
            WHERE id = %s
        """, (request_id,))
        
        conn.commit()
        
        # Логируем удаление
        log_action('delete', 'chat_request', request_id, 
                  f'Пользователь удалил запрос',
                  f'Удалён запрос ID: {request_id}')
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': '✅ Запрос удалён!'
        })
        
    except Exception as e:
        print(f"Ошибка в delete_chat_request: {e}")
        return jsonify({'success': False, 'error': 'Ошибка сервера'}), 500

if __name__ == '__main__':
    print("🚀 Запускаем Smart Beef Farm...")
    print("📊 Откройте в браузере: http://localhost:5000")
    print("👤 Тестовые пользователи: admin/admin123, manager/manager123, worker/worker123")
    app.run(debug=True, host='0.0.0.0', port=5000)