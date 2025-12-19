import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes, ConversationHandler
)
import psycopg2
from decimal import Decimal
from datetime import datetime
import json

# -------------------------
# НАСТРОЙКИ БАЗЫ ДАННЫХ
# -------------------------
def get_db_connection():
    return psycopg2.connect(
        dbname="smart_beef_farm",
        user="postgres",
        password="1234",
        host="localhost",
        port="5432"
    )

# -------------------------
# ЛОГИ
# -------------------------
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Состояния диалога
(SELECTING_ACTION, SELECTING_PRODUCT, ENTERING_QUANTITY, 
 ENTERING_NAME, ENTERING_PHONE, VIEW_ORDERS_NAME, VIEW_ORDERS_PHONE) = range(7)

# Словарь для хранения временных данных
user_sessions = {}

# -------------------------
# ФОРМАТИРОВАНИЕ КОЛИЧЕСТВА
# -------------------------
def format_quantity(quantity):
    """Форматирует количество - если больше 1000, показывает в тоннах"""
    if quantity >= 1000:
        return f"{quantity/1000:.1f} тонн"
    return f"{quantity:.0f} кг"

# -------------------------
# /start
# -------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /start - главное меню"""
    keyboard = [
        [InlineKeyboardButton("🥩 Разделанная туша", callback_data="order_cut")],
        [InlineKeyboardButton("🐄 Целая туша", callback_data="order_whole")],
        [InlineKeyboardButton("🌾 Зерно", callback_data="order_grain")],
        [InlineKeyboardButton("🌿 Сено", callback_data="order_hay")],
        [InlineKeyboardButton("📋 Мои заказы", callback_data="my_orders")]
    ]
    
    if update.message:
        await update.message.reply_text(
            "👋 Добро пожаловать в Agro Farm!\n"
            "Выберите продукт для заказа:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    else:
        await update.callback_query.edit_message_text(
            "👋 Добро пожаловать в Agro Farm!\n"
            "Выберите продукт для заказа:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    return SELECTING_ACTION

# -------------------------
# ВЫБОР ТИПА ТОВАРА
# -------------------------
async def order_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора типа товара"""
    query = update.callback_query
    await query.answer()
    
    # Обработка кнопки "Назад"
    if query.data == "back":
        await start(update, context)
        return SELECTING_ACTION
    
    user_id = query.from_user.id
    
    if query.data == "my_orders":
        # Начинаем процесс просмотра заказов
        user_sessions[user_id] = {'action': 'view_orders'}
        await query.edit_message_text(
            "📋 Для просмотра ваших заказов введите ваше имя:\n"
            "(Имя должно совпадать с именем, указанным при оформлении заказа)"
        )
        return VIEW_ORDERS_NAME
    
    # Если это не "Мои заказы", то начинаем новый заказ
    user_sessions[user_id] = {'action': 'new_order'}
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    if query.data == "order_cut":
        # Получаем список разделанных туш из таблицы meat_carcasses
        cursor.execute("""
            SELECT id, breed, carcass_weight, price, status 
            FROM meat_carcasses 
            WHERE status = 'в наличии'
            ORDER BY price
        """)
        carcasses = cursor.fetchall()
        
        if not carcasses:
            await query.edit_message_text("❌ Разделанных туш нет в наличии.")
            cursor.close()
            conn.close()
            return SELECTING_ACTION
        
        keyboard = []
        for carcass in carcasses:
            carcass_id, breed, weight, price, status = carcass
            button_text = f"{breed} - {weight}кг ({price}₸)"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"cut_{carcass_id}")])
        
        keyboard.append([InlineKeyboardButton("↩️ Назад", callback_data="back")])
        
        await query.edit_message_text(
            "🥩 Выберите разделанную тушу:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        user_sessions[user_id]['order_type'] = 'разделанная_туша'
        cursor.close()
        conn.close()
        return SELECTING_PRODUCT
    
    elif query.data == "order_whole":
        # Получаем список живых животных
        cursor.execute("""
            SELECT id, name, breed, current_weight, price, status 
            FROM animals 
            WHERE status = 'готов к забою' AND price IS NOT NULL
            ORDER BY price
        """)
        animals = cursor.fetchall()
        
        if not animals:
            await query.edit_message_text("❌ Живых туш нет в наличии.")
            cursor.close()
            conn.close()
            return SELECTING_ACTION
        
        keyboard = []
        for animal in animals:
            animal_id, name, breed, weight, price, status = animal
            display_name = name or f"{breed} #{animal_id}"
            button_text = f"{display_name} - {weight}кг ({price}₸)"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"whole_{animal_id}")])
        
        keyboard.append([InlineKeyboardButton("↩️ Назад", callback_data="back")])
        
        await query.edit_message_text(
            "🐄 Выберите целую тушу:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        user_sessions[user_id]['order_type'] = 'живая_туша'
        cursor.close()
        conn.close()
        return SELECTING_PRODUCT
    
    elif query.data == "order_grain":
        # Получаем зерновые корма
        cursor.execute("""
            SELECT product_type, current_quantity, price_per_unit, unit 
            FROM storage 
            WHERE feed_category = 'зерновой корм' AND current_quantity > 0
            ORDER BY product_type
        """)
        grains = cursor.fetchall()
        
        if not grains:
            await query.edit_message_text("❌ Зерна нет в наличии.")
            cursor.close()
            conn.close()
            return SELECTING_ACTION
        
        keyboard = []
        for grain in grains:
            product_type, quantity, price, unit = grain
            button_text = f"{product_type} - {price}₸/{unit} ({quantity} {unit})"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"grain_{product_type}")])
        
        keyboard.append([InlineKeyboardButton("↩️ Назад", callback_data="back")])
        
        await query.edit_message_text(
            "🌾 Выберите зерно:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        user_sessions[user_id]['order_type'] = 'зерно'
        cursor.close()
        conn.close()
        return SELECTING_PRODUCT
    
    elif query.data == "order_hay":
        # Получаем сено
        cursor.execute("""
            SELECT product_type, current_quantity, price_per_unit, unit 
            FROM storage 
            WHERE feed_category = 'сено' AND current_quantity > 0
            ORDER BY product_type
        """)
        hays = cursor.fetchall()
        
        if not hays:
            await query.edit_message_text("❌ Сена нет в наличии.")
            cursor.close()
            conn.close()
            return SELECTING_ACTION
        
        keyboard = []
        for hay in hays:
            product_type, quantity, price, unit = hay
            button_text = f"{product_type} - {price}₸/{unit} ({quantity} {unit})"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"hay_{product_type}")])
        
        keyboard.append([InlineKeyboardButton("↩️ Назад", callback_data="back")])
        
        await query.edit_message_text(
            "🌿 Выберите сено:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        user_sessions[user_id]['order_type'] = 'сено'
        cursor.close()
        conn.close()
        return SELECTING_PRODUCT
    
    cursor.close()
    conn.close()
    return SELECTING_ACTION

# -------------------------
# ВЫБОР КОНКРЕТНОГО ТОВАРА
# -------------------------
async def select_product_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выбор конкретного товара"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    data = query.data
    
    if data == "back":
        await start(update, context)
        return SELECTING_ACTION
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Обработка разделанной туши из таблицы meat_carcasses
        if data.startswith("cut_"):
            carcass_id = int(data.split("_")[1])
            cursor.execute("""
                SELECT breed, carcass_weight, price, description 
                FROM meat_carcasses 
                WHERE id = %s
            """, (carcass_id,))
            carcass = cursor.fetchone()
            
            if carcass:
                breed, weight, price, description = carcass
                total_price = float(price)
                
                user_sessions[user_id].update({
                    'product_id': carcass_id,
                    'product_name': f"Разделанная туша ({breed})",
                    'price': float(price),
                    'weight': float(weight),
                    'unit': 'шт',
                    'quantity': 1,
                    'total_price': total_price
                })
                
                message_text = (
                    f"🥩 Разделанная туша:\n"
                    f"Порода: {breed}\n"
                    f"Вес: {weight} кг\n"
                    f"Цена: {price} ₸\n"
                    f"Итого: {total_price} ₸\n"
                )
                if description:
                    message_text += f"Описание: {description}\n"
                
                await query.edit_message_text(f"{message_text}\nВведите ваше имя:")
                return ENTERING_NAME
        
        # Обработка целой туши
        elif data.startswith("whole_"):
            animal_id = int(data.split("_")[1])
            cursor.execute("""
                SELECT name, breed, current_weight, price 
                FROM animals 
                WHERE id = %s
            """, (animal_id,))
            animal = cursor.fetchone()
            
            if animal:
                name, breed, weight, price = animal
                display_name = name or f"{breed} #{animal_id}"
                total_price = float(price)
                
                user_sessions[user_id].update({
                    'product_id': animal_id,
                    'product_name': f"Целая туша ({display_name})",
                    'price': float(price),
                    'weight': float(weight),
                    'unit': 'шт',
                    'quantity': 1,
                    'total_price': total_price
                })
                
                message_text = (
                    f"🐄 Целая туша:\n"
                    f"Имя: {display_name}\n"
                    f"Порода: {breed}\n"
                    f"Вес: {weight} кг\n"
                    f"Цена: {price} ₸\n"
                    f"Итого: {total_price} ₸\n"
                )
                
                await query.edit_message_text(f"{message_text}\nВведите ваше имя:")
                return ENTERING_NAME
        
        # Обработка зерна
        elif data.startswith("grain_"):
            product_type = data.split("_")[1]
            cursor.execute("""
                SELECT product_type, current_quantity, price_per_unit, unit 
                FROM storage 
                WHERE product_type = %s AND feed_category = 'зерновой корм'
            """, (product_type,))
            grain = cursor.fetchone()
            
            if grain:
                product_type, quantity, price, unit = grain
                
                user_sessions[user_id].update({
                    'product_type': product_type,
                    'product_name': f"Зерно ({product_type})",
                    'price': float(price),
                    'unit': unit,
                    'available': float(quantity)
                })
                
                message_text = (
                    f"🌾 Зерно: {product_type}\n"
                    f"В наличии: {quantity} {unit}\n"
                    f"Цена: {price} ₸/{unit}\n"
                )
                
                await query.edit_message_text(f"{message_text}\nВведите количество ({unit}):")
                return ENTERING_QUANTITY
        
        # Обработка сена
        elif data.startswith("hay_"):
            product_type = data.split("_")[1]
            cursor.execute("""
                SELECT product_type, current_quantity, price_per_unit, unit 
                FROM storage 
                WHERE product_type = %s AND feed_category = 'сено'
            """, (product_type,))
            hay = cursor.fetchone()
            
            if hay:
                product_type, quantity, price, unit = hay
                
                user_sessions[user_id].update({
                    'product_type': product_type,
                    'product_name': f"Сено ({product_type})",
                    'price': float(price),
                    'unit': unit,
                    'available': float(quantity)
                })
                
                message_text = (
                    f"🌿 Сено: {product_type}\n"
                    f"В наличии: {quantity} {unit}\n"
                    f"Цена: {price} ₸/{unit}\n"
                )
                
                await query.edit_message_text(f"{message_text}\nВведите количество ({unit}):")
                return ENTERING_QUANTITY
        
    except Exception as e:
        logger.error(f"Ошибка при выборе товара: {e}")
        await query.edit_message_text("❌ Произошла ошибка. Попробуйте снова.")
    finally:
        cursor.close()
        conn.close()
    
    return SELECTING_PRODUCT

# -------------------------
# ВВОД КОЛИЧЕСТВА (для заказа)
# -------------------------
async def enter_quantity(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ввод количества для зерна/сена"""
    user_id = update.message.from_user.id
    
    if user_id not in user_sessions:
        await update.message.reply_text("❌ Сессия устарела. Начните заново с /start")
        return ConversationHandler.END
    
    try:
        quantity = float(update.message.text.strip())
        if quantity <= 0:
            await update.message.reply_text("❌ Количество должно быть больше 0. Попробуйте снова:")
            return ENTERING_QUANTITY
        
        # Проверяем доступное количество
        available = user_sessions[user_id].get('available', float('inf'))
        if quantity > available:
            await update.message.reply_text(
                f"❌ Максимально доступно: {available} {user_sessions[user_id]['unit']}\n"
                f"Введите меньшее количество:"
            )
            return ENTERING_QUANTITY
        
        # Рассчитываем общую стоимость
        price = user_sessions[user_id]['price']
        total_price = quantity * price
        
        user_sessions[user_id].update({
            'quantity': quantity,
            'total_price': total_price
        })
        
        await update.message.reply_text(
            f"✅ Количество: {quantity} {user_sessions[user_id]['unit']}\n"
            f"💰 Итого: {total_price:.2f} ₸\n\n"
            f"Введите ваше имя:"
        )
        return ENTERING_NAME
        
    except ValueError:
        await update.message.reply_text("❌ Пожалуйста, введите число. Попробуйте снова:")
        return ENTERING_QUANTITY

# -------------------------
# ВВОД ИМЕНИ (для заказа)
# -------------------------
async def enter_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ввод имени для оформления заказа"""
    user_id = update.message.from_user.id
    
    if user_id not in user_sessions:
        await update.message.reply_text("❌ Сессия устарела. Начните заново с /start")
        return ConversationHandler.END
    
    name = update.message.text.strip()
    if len(name) < 2:
        await update.message.reply_text("❌ Имя должно содержать минимум 2 символа. Попробуйте снова:")
        return ENTERING_NAME
    
    user_sessions[user_id]['customer_name'] = name
    
    await update.message.reply_text(
        "📞 Введите ваш номер телефона:\n"
        "(например: 87011234567)"
    )
    return ENTERING_PHONE

# -------------------------
# ВВОД ТЕЛЕФОНА (для заказа)
# -------------------------
async def enter_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ввод телефона для оформления заказа"""
    user_id = update.message.from_user.id
    
    if user_id not in user_sessions:
        await update.message.reply_text("❌ Сессия устарела. Начните заново с /start")
        return ConversationHandler.END
    
    phone = update.message.text.strip()
    
    # Очистка номера телефона
    phone_digits = ''.join(filter(str.isdigit, phone))
    
    if len(phone_digits) < 10:
        await update.message.reply_text("❌ Некорректный номер телефона. Попробуйте снова:")
        return ENTERING_PHONE
    
    # Форматирование номера (Казахстан)
    if phone_digits.startswith('7') and len(phone_digits) == 11:
        formatted_phone = f"+{phone_digits}"
    elif len(phone_digits) == 10:
        formatted_phone = f"+7{phone_digits}"
    elif phone_digits.startswith('87') and len(phone_digits) == 11:
        formatted_phone = f"+7{phone_digits[1:]}"
    else:
        formatted_phone = phone_digits
    
    user_sessions[user_id]['phone'] = formatted_phone
    
    # Сохранение заказа в базу данных
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        session = user_sessions[user_id]
        
        # Подготовка данных для сохранения
        product_id = session.get('product_id')
        product_type = session.get('product_type')
        product_name = session['product_name']
        order_type = session['order_type']
        quantity = session.get('quantity', 1)
        price = session['price']
        total_price = session['total_price']
        customer_name = session['customer_name']
        telegram_username = f"@{update.message.from_user.username}" if update.message.from_user.username else None
        
        # Сохраняем заказ
        cursor.execute("""
            INSERT INTO orders (
                customer_name, phone, telegram_username,
                order_type, product_id, product_name,
                quantity, price, total_price, status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'новый')
        """, (
            customer_name,
            formatted_phone,
            telegram_username,
            order_type,
            product_id,
            product_name,
            quantity,
            price,
            total_price
        ))
        
        # Обновляем количество на складе для зерна/сена
        if order_type in ['зерно', 'сено'] and product_type:
            cursor.execute("""
                UPDATE storage 
                SET current_quantity = current_quantity - %s
                WHERE product_type = %s
            """, (Decimal(str(quantity)), product_type))
        
        # Обновляем статус для туш
        if order_type == 'разделанная_туша' and product_id:
            cursor.execute("""
                UPDATE meat_carcasses 
                SET status = 'продано'
                WHERE id = %s
            """, (product_id,))
        elif order_type == 'живая_туша' and product_id:
            cursor.execute("""
                UPDATE animals 
                SET status = 'продан'
                WHERE id = %s
            """, (product_id,))
        
        conn.commit()
        
        # Отправляем подтверждение
        order_type_display = {
            'живая_туша': '🐄 Целая туша',
            'разделанная_туша': '🥩 Разделанная туша',
            'зерно': '🌾 Зерно',
            'сено': '🌿 Сено'
        }.get(order_type, order_type)
        
        quantity_display = f"{quantity} шт" if order_type in ['живая_туша', 'разделанная_туша'] else f"{quantity} {session.get('unit', 'кг')}"
        
        await update.message.reply_text(
            f"✅ Заказ успешно оформлен!\n\n"
            f"📦 Тип: {order_type_display}\n"
            f"🏷️ Товар: {product_name}\n"
            f"📊 Количество: {quantity_display}\n"
            f"💰 Сумма: {total_price:.2f} ₸\n"
            f"👤 Имя: {customer_name}\n"
            f"📞 Телефон: {formatted_phone}\n\n"
            f"📱 Наш менеджер свяжется с вами в ближайшее время!\n"
            f"Для нового заказа нажмите /start"
        )
        
    except Exception as e:
        logger.error(f"Ошибка при сохранении заказа: {e}")
        await update.message.reply_text(
            "❌ Произошла ошибка при оформлении заказа. Пожалуйста, попробуйте позже."
        )
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
        # Очищаем сессию
        if user_id in user_sessions:
            del user_sessions[user_id]
    
    return ConversationHandler.END

# -------------------------
# ВВОД ИМЕНИ (для просмотра заказов)
# -------------------------
async def view_orders_enter_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ввод имени для просмотра заказов"""
    user_id = update.message.from_user.id
    
    if user_id not in user_sessions or user_sessions[user_id].get('action') != 'view_orders':
        await update.message.reply_text("❌ Сессия устарела. Начните заново с /start")
        return ConversationHandler.END
    
    name = update.message.text.strip()
    if len(name) < 2:
        await update.message.reply_text("❌ Имя должно содержать минимум 2 символа. Попробуйте снова:")
        return VIEW_ORDERS_NAME
    
    user_sessions[user_id]['search_name'] = name
    
    await update.message.reply_text(
        "📞 Теперь введите номер телефона, указанный при заказе:\n"
        "(например: 8700-000-0000)"
    )
    return VIEW_ORDERS_PHONE

# -------------------------
# ВВОД ТЕЛЕФОНА (для просмотра заказов)
# -------------------------
async def view_orders_enter_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ввод телефона для просмотра заказов"""
    user_id = update.message.from_user.id
    
    if user_id not in user_sessions or user_sessions[user_id].get('action') != 'view_orders':
        await update.message.reply_text("❌ Сессия устарела. Начните заново с /start")
        return ConversationHandler.END
    
    phone = update.message.text.strip()
    
    # Очистка номера телефона
    phone_digits = ''.join(filter(str.isdigit, phone))
    
    if len(phone_digits) < 10:
        await update.message.reply_text("❌ Некорректный номер телефона. Попробуйте снова:")
        return VIEW_ORDERS_PHONE
    
    # Форматирование номера (Казахстан)
    if phone_digits.startswith('7') and len(phone_digits) == 11:
        formatted_phone = f"+{phone_digits}"
    elif len(phone_digits) == 10:
        formatted_phone = f"+7{phone_digits}"
    elif phone_digits.startswith('87') and len(phone_digits) == 11:
        formatted_phone = f"+7{phone_digits[1:]}"
    else:
        formatted_phone = phone_digits
    
    # Поиск заказов по имени и телефону
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        search_name = user_sessions[user_id]['search_name']
        
        cursor.execute("""
            SELECT id, product_name, quantity, total_price, status, 
                   created_at, order_type, price, notes
            FROM orders
            WHERE customer_name ILIKE %s AND phone = %s
            ORDER BY created_at DESC
            LIMIT 20
        """, (f"%{search_name}%", formatted_phone))
        
        orders = cursor.fetchall()
        
        if not orders:
            await update.message.reply_text(
                f"📭 Заказы не найдены.\n"
                f"Имя: {search_name}\n"
                f"Телефон: {formatted_phone}\n\n"
                f"Проверьте правильность введенных данных или нажмите /start для нового заказа."
            )
        else:
            text = f"📋 Ваши заказы:\n\n"
            text += f"👤 Имя: {search_name}\n"
            text += f"📞 Телефон: {formatted_phone}\n"
            text += f"📊 Всего заказов: {len(orders)}\n"
            text += f"{'='*40}\n\n"
            
            total_amount = 0
            for order in orders:
                order_id, product_name, quantity, total_price, status, created_at, order_type, price, notes = order
                
                status_emoji = {
                    'новый': '🆕',
                    'в_обработке': '⚙️',
                    'выполнен': '✅',
                    'отменен': '❌'
                }.get(status, '❓')
                
                order_type_display = {
                    'живая_туша': '🐄 Целая туша',
                    'разделанная_туша': '🥩 Разделанная туша',
                    'зерно': '🌾 Зерно',
                    'сено': '🌿 Сено'
                }.get(order_type, order_type)
                
                if hasattr(created_at, 'strftime'):
                    created_str = created_at.strftime("%d.%m.%Y %H:%M")
                else:
                    created_str = str(created_at)
                
                quantity_display = f"{quantity} шт" if order_type in ['живая_туша', 'разделанная_туша'] else f"{quantity} кг"
                
                text += f"🆔 Заказ #{order_id}\n"
                text += f"📦 Тип: {order_type_display}\n"
                text += f"🏷️ Товар: {product_name}\n"
                text += f"📊 Количество: {quantity_display}\n"
                text += f"💰 Цена за ед.: {float(price):.2f} ₸\n"
                text += f"💰 Итого: {float(total_price):.2f} ₸\n"
                text += f"📋 Статус: {status_emoji} {status}\n"
                if notes:
                    text += f"📝 Примечания: {notes}\n"
                text += f"📅 Дата заказа: {created_str}\n"
                text += f"{'-'*40}\n\n"
                
                total_amount += float(total_price)
            
            text += f"\n💰 Общая сумма всех заказов: {total_amount:.2f} ₸\n\n"
            text += "Для нового заказа нажмите /start"
            
            await update.message.reply_text(text)
        
    except Exception as e:
        logger.error(f"Ошибка при поиске заказов: {e}")
        await update.message.reply_text(
            "❌ Произошла ошибка при поиске заказов. Пожалуйста, попробуйте позже."
        )
    finally:
        cursor.close()
        conn.close()
        # Очищаем сессию
        if user_id in user_sessions:
            del user_sessions[user_id]
    
    return ConversationHandler.END

# -------------------------
# ОТМЕНА
# -------------------------
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отмена диалога"""
    user_id = update.message.from_user.id
    if user_id in user_sessions:
        del user_sessions[user_id]
    
    await update.message.reply_text(
        "❌ Диалог отменен.\n"
        "Для начала нового заказа нажмите /start"
    )
    return ConversationHandler.END

# -------------------------
# ЗАПУСК БОТА
# -------------------------
def main():
    """Основная функция запуска бота"""
    # Вставьте ваш токен от @BotFather
    TOKEN = "8524485458:AAEccAWCIrSK_IgcnQWV9w9Lx_jIvGpDQoc"
    
    # Создаем приложение
    application = Application.builder().token(TOKEN).build()
    
    # Добавляем обработчик ошибок
    async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
        logger.error(f"Ошибка: {context.error}")
    
    application.add_error_handler(error_handler)
    
    # Создаем ConversationHandler
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            SELECTING_ACTION: [
                CallbackQueryHandler(order_handler, pattern="^(order_cut|order_whole|order_grain|order_hay|my_orders)$"),
                CallbackQueryHandler(start, pattern="^back$")
            ],
            SELECTING_PRODUCT: [
                CallbackQueryHandler(select_product_handler, pattern="^(cut|whole|grain|hay)_"),
                CallbackQueryHandler(start, pattern="^back$")
            ],
            ENTERING_QUANTITY: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, enter_quantity)
            ],
            ENTERING_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, enter_name)
            ],
            ENTERING_PHONE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, enter_phone)
            ],
            VIEW_ORDERS_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, view_orders_enter_name)
            ],
            VIEW_ORDERS_PHONE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, view_orders_enter_phone)
            ]
        },
        fallbacks=[CommandHandler('cancel', cancel)]
    )
    
    # Добавляем обработчик
    application.add_handler(conv_handler)
    
    # Запускаем бота
    print("🤖 Бот запускается...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()