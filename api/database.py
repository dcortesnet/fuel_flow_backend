import psycopg2

def get_connection():
    try:
        conn = psycopg2.connect(
            dbname='',
            user='',
            password='',
            host='',
            port='',
        )
        return conn
    except psycopg2.Error as e:
        return None

def get_tipo_combustible_id(tipo_combustible):
    conn = get_connection()
    if not conn:
        return None
    
    cursor = conn.cursor()
    try:
        tipo_map = {
            'Regular': 3,
            'Diesel': 4,
            'Premium': 5
        }
        tipo_id = tipo_map.get(tipo_combustible)
        
        if tipo_id:
            cursor.execute("""
                SELECT tipo_combustible_id 
                FROM tipo_combustible 
                WHERE tipo_combustible_id = %s
            """, (tipo_id,))
            result = cursor.fetchone()
            if result:
                return result[0]
        
        cursor.execute("""
            SELECT tipo_combustible_id 
            FROM tipo_combustible 
            WHERE tipo_combustible::text ILIKE %s
        """, (f'%{tipo_combustible}%',))
        result = cursor.fetchone()
        return result[0] if result else 3
    finally:
        cursor.close()
        conn.close()

def get_estado_pedido_id(estado):
    conn = get_connection()
    if not conn:
        return 1
    
    cursor = conn.cursor()
    try:
        estado_map = {
            'pendiente': 1,
            'en_ruta': 2,
            'completado': 3,
            'cancelado': 4 if estado else 1
        }
        estado_id = estado_map.get(estado.lower())
        
        if estado_id:
            cursor.execute("""
                SELECT estado_pedido_id 
                FROM estado_pedido 
                WHERE estado_pedido_id = %s
            """, (estado_id,))
            result = cursor.fetchone()
            if result:
                return result[0]
        
        return 1
    finally:
        cursor.close()
        conn.close()

def get_or_create_cliente(nombre, telefono, direccion):
    conn = get_connection()
    if not conn:
        raise Exception("No se pudo conectar a la BD")
    
    cursor = conn.cursor()
    try:
        telefono_limpio = ''.join(filter(str.isdigit, str(telefono)))

        cursor.execute("""
            SELECT cliente_id FROM cliente 
            WHERE nombre = %s AND direccion = %s
        """, (nombre, direccion))
        result = cursor.fetchone()
        
        if result:
            cliente_id = result[0]
            cursor.execute("""
                UPDATE cliente 
                SET nombre = %s, telefono = %s, direccion = %s 
                WHERE cliente_id = %s
            """, (nombre, telefono_limpio, direccion, cliente_id))
            conn.commit()
            return cliente_id
        
        cursor.execute("""
            INSERT INTO cliente (nombre, telefono, direccion)
            VALUES (%s, %s, %s)
            RETURNING cliente_id
        """, (nombre, telefono_limpio, direccion))
        cliente_id = cursor.fetchone()[0]
        conn.commit()
        return cliente_id
    except psycopg2.Error as e:
        conn.rollback()
        raise Exception(f"Error en get_or_create_cliente: {e}")
    finally:
        cursor.close()
        conn.close()

# ============ FUNCIONES PRINCIPALES ============

def init_db():
    conn = get_connection()
    if not conn:
        return
    
    cursor = conn.cursor()
    
    try:
        # Verificar que existe la tabla administrador
        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_name = 'administrador'
        """)
        
        if cursor.fetchone()[0] == 0:
            return
        
        conn.commit()
        
        # Crear usuario admin por defecto si no existe
        cursor.execute("SELECT COUNT(*) FROM administrador WHERE usuario = 'admin'")
        if cursor.fetchone()[0] == 0:
            cursor.execute("""
                INSERT INTO administrador (usuario, contraseña) 
                VALUES ('admin', 'admin123')
            """)
            conn.commit()
            
    except psycopg2.Error as e:
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def get_pedidos(estado=None, limit=None, offset=None):
    """Obtiene pedidos de la base de datos con JOIN a las tablas relacionadas"""
    conn = get_connection()
    if not conn:
        return []
    
    cursor = conn.cursor()
    try:
        query = """
            SELECT 
                p.pedidos_id as id,
                p.cantidad_combustible as cantidad,
                p.fecha_hora_creacion as fecha_pedido,
                p.fecha_entrega as fecha_completado,
                p.observacion as observaciones,
                p.urgencia::text as nivel_urgencia,
                c.nombre as nombre_cliente,
                c.telefono::text as telefono,
                c.direccion as direccion,
                tc.tipo_combustible::text as tipo_combustible,
                ep.estado_pedido::text as estado
            FROM pedidos p
            LEFT JOIN cliente c ON p.cliente_id = c.cliente_id
            LEFT JOIN tipo_combustible tc ON p.tipo_combustible_id = tc.tipo_combustible_id
            LEFT JOIN estado_pedido ep ON p.estado_pedido_id = ep.estado_pedido_id
        """
        params = []
        
        if estado:
            # Normalizar estado según valores en la BD
            estado_normalizado = {
                'pendiente': 'Pendiente',
                'en_ruta': 'En_Ruta',
                'completado': 'Completado',
                'cancelado': 'Cancelado'
            }.get(estado.lower(), estado)
            query += " WHERE ep.estado_pedido::text = %s"
            params.append(estado_normalizado)
        
        query += " ORDER BY p.fecha_hora_creacion DESC"
        
        if limit:
            query += " LIMIT %s"
            params.append(limit)
        
        if offset:
            query += " OFFSET %s"
            params.append(offset)
        
        cursor.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        results = cursor.fetchall()
        
        pedidos = []
        for row in results:
            pedido = dict(zip(columns, row))
            # Asegurarse de que todos los campos necesarios estén presentes
            if 'estado' not in pedido or pedido['estado'] is None:
                pedido['estado'] = 'pendiente'
            pedidos.append(pedido)
        
        return pedidos
    except psycopg2.Error as e:
        return []
    finally:
        cursor.close()
        conn.close()

def get_pedido_by_id(id):
    """Obtiene un pedido por su ID con JOIN a las tablas relacionadas"""
    conn = get_connection()
    if not conn:
        return None
    
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT 
                p.pedidos_id as id,
                p.cantidad_combustible as cantidad,
                p.fecha_hora_creacion as fecha_pedido,
                p.fecha_entrega as fecha_completado,
                p.observacion as observaciones,
                p.urgencia::text as nivel_urgencia,
                c.nombre as nombre_cliente,
                c.telefono::text as telefono,
                c.direccion as direccion,
                tc.tipo_combustible::text as tipo_combustible,
                ep.estado_pedido::text as estado
            FROM pedidos p
            LEFT JOIN cliente c ON p.cliente_id = c.cliente_id
            LEFT JOIN tipo_combustible tc ON p.tipo_combustible_id = tc.tipo_combustible_id
            LEFT JOIN estado_pedido ep ON p.estado_pedido_id = ep.estado_pedido_id
            WHERE p.pedidos_id = %s
        """, (id,))
        
        columns = [desc[0] for desc in cursor.description]
        row = cursor.fetchone()
        
        if row:
            pedido = dict(zip(columns, row))
            # Asegurar que el estado esté presente
            if 'estado' not in pedido or pedido['estado'] is None:
                pedido['estado'] = 'pendiente'
            return pedido
        return None
    except psycopg2.Error as e:
        return None
    finally:
        cursor.close()
        conn.close()

def create_pedido(pedido_data):
    conn = get_connection()
    if not conn:
        return None
    
    cursor = conn.cursor()
    try:
        # Obtener IDs de las tablas de referencia
        cliente_id = get_or_create_cliente(
            pedido_data['nombre_cliente'],
            pedido_data['telefono'],
            pedido_data['direccion']
        )
        
        if not cliente_id:
            raise Exception("No se pudo obtener/crear cliente")
        
        tipo_combustible_id = get_tipo_combustible_id(pedido_data['tipo_combustible'])
        
        if not tipo_combustible_id:
            raise Exception("No se pudo obtener tipo_combustible_id")
        
        # Convertir cantidad a entero (en tu BD es integer, no decimal)
        cantidad = int(float(pedido_data['cantidad']))
        
        # Mapear urgencia
        urgencia_map = {
            'normal': 'Normal',
            'urgente': 'Urgente',
            'critico': 'Critico'
        }
        urgencia = urgencia_map.get(pedido_data.get('nivel_urgencia', 'normal'), 'Normal')
        
        # Insertar pedido
        cursor.execute("""
            INSERT INTO pedidos (
                cliente_id, tipo_combustible_id, estado_pedido_id,
                cantidad_combustible, urgencia, observacion, fecha_hora_creacion
            ) VALUES (%s, %s, %s, %s, %s::urgencia_pedido, %s, CURRENT_TIMESTAMP)
            RETURNING pedidos_id
        """, (
            cliente_id,
            tipo_combustible_id,
            1,  # Estado inicial: Pendiente
            cantidad,
            urgencia,
            pedido_data.get('observaciones', '')
        ))
        
        pedido_id = cursor.fetchone()[0]
        conn.commit()
        
        return get_pedido_by_id(pedido_id)
    except (psycopg2.Error, ValueError) as e:
        conn.rollback()
        return None
    finally:
        cursor.close()
        conn.close()

def update_pedido(id, pedido_data):
    """Actualiza un pedido existente"""
    conn = get_connection()
    if not conn:
        return None
    
    cursor = conn.cursor()
    try:
        cliente_id = get_or_create_cliente(
            pedido_data['nombre_cliente'],
            pedido_data['telefono'],
            pedido_data['direccion']
        )

        tipo_combustible_id = get_tipo_combustible_id(pedido_data['tipo_combustible'])

        cantidad = int(float(pedido_data['cantidad']))

        urgencia_map = {
            'normal': 'Normal',
            'urgente': 'Urgente',
            'critico': 'Critico'
        }
        urgencia = urgencia_map.get(pedido_data.get('nivel_urgencia', 'normal'), 'Normal')

        cursor.execute("""
            UPDATE pedidos SET
                cliente_id = %s,
                tipo_combustible_id = %s,
                cantidad_combustible = %s,
                urgencia = %s::urgencia_pedido,
                observacion = %s
            WHERE pedidos_id = %s
        """, (
            cliente_id,
            tipo_combustible_id,
            cantidad,
            urgencia,
            pedido_data.get('observaciones', ''),
            id
        ))
        
        conn.commit()
        return get_pedido_by_id(id)
    except (psycopg2.Error, ValueError) as e:
        conn.rollback()
        return None
    finally:
        cursor.close()
        conn.close()

def delete_pedido(id):
    """Elimina un pedido"""
    conn = get_connection()
    if not conn:
        return False
    
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM pedidos WHERE pedidos_id = %s", (id,))
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

def cambiar_estado_pedido(id, nuevo_estado):
    """Cambia el estado de un pedido"""
    conn = get_connection()
    if not conn:
        return None
    
    cursor = conn.cursor()
    try:
        # Convertir estado a ID
        estado_id = get_estado_pedido_id(nuevo_estado)
        
        update_query = "UPDATE pedidos SET estado_pedido_id = %s"
        
        if nuevo_estado.lower() == 'completado' or nuevo_estado == 'Completado':
            update_query += ", fecha_entrega = CURRENT_TIMESTAMP"
        
        update_query += " WHERE pedidos_id = %s"
        
        cursor.execute(update_query, (estado_id, id))
        conn.commit()
        return get_pedido_by_id(id)
    except psycopg2.Error as e:
        conn.rollback()
        return None
    finally:
        cursor.close()
        conn.close()

def get_estadisticas():
    """Obtiene estadísticas de pedidos"""
    conn = get_connection()
    if not conn:
        return {}
    
    cursor = conn.cursor()
    try:
        # Pedidos pendientes
        cursor.execute("""
            SELECT COUNT(*) FROM pedidos p
            JOIN estado_pedido ep ON p.estado_pedido_id = ep.estado_pedido_id
            WHERE ep.estado_pedido::text = 'Pendiente'
        """)
        pendientes = cursor.fetchone()[0]
        
        # Completados hoy
        cursor.execute("""
            SELECT COUNT(*) FROM pedidos 
            WHERE fecha_entrega IS NOT NULL 
            AND DATE(fecha_entrega) = CURRENT_DATE
        """)
        completados_hoy = cursor.fetchone()[0]
        
        # Completados esta semana
        cursor.execute("""
            SELECT COUNT(*) FROM pedidos 
            WHERE fecha_entrega IS NOT NULL
            AND fecha_entrega >= DATE_TRUNC('week', CURRENT_DATE)
        """)
        completados_semana = cursor.fetchone()[0]
        
        # Top combustibles
        cursor.execute("""
            SELECT tc.tipo_combustible::text, COUNT(*) as cantidad
            FROM pedidos p
            JOIN tipo_combustible tc ON p.tipo_combustible_id = tc.tipo_combustible_id
            GROUP BY tc.tipo_combustible
            ORDER BY cantidad DESC
            LIMIT 5
        """)
        top_combustibles = [
            {'tipo': row[0], 'cantidad': row[1]}
            for row in cursor.fetchall()
        ]
        
        return {
            'pendientes': pendientes,
            'completadosHoy': completados_hoy,
            'completadosSemana': completados_semana,
            'combustibleTop': top_combustibles
        }
    except psycopg2.Error as e:
        return {}
    finally:
        cursor.close()
        conn.close()

def buscar_pedidos(termino):
    """Busca pedidos por término"""
    conn = get_connection()
    if not conn:
        return []
    
    cursor = conn.cursor()
    try:
        query = """
            SELECT 
                p.pedidos_id as id,
                p.cantidad_combustible as cantidad,
                p.fecha_hora_creacion as fecha_pedido,
                p.fecha_entrega as fecha_completado,
                p.observacion as observaciones,
                p.urgencia::text as nivel_urgencia,
                c.nombre as nombre_cliente,
                c.telefono::text as telefono,
                c.direccion as direccion,
                tc.tipo_combustible::text as tipo_combustible,
                ep.estado_pedido::text as estado
            FROM pedidos p
            LEFT JOIN cliente c ON p.cliente_id = c.cliente_id
            LEFT JOIN tipo_combustible tc ON p.tipo_combustible_id = tc.tipo_combustible_id
            LEFT JOIN estado_pedido ep ON p.estado_pedido_id = ep.estado_pedido_id
            WHERE c.nombre ILIKE %s OR c.direccion ILIKE %s
            ORDER BY p.fecha_hora_creacion DESC
        """
        termino_search = f"%{termino}%"
        cursor.execute(query, (termino_search, termino_search))
        
        columns = [desc[0] for desc in cursor.description]
        results = cursor.fetchall()
        
        pedidos = []
        for row in results:
            pedido = dict(zip(columns, row))
            if 'estado' not in pedido or pedido['estado'] is None:
                pedido['estado'] = 'pendiente'
            pedidos.append(pedido)
        
        return pedidos
    except psycopg2.Error as e:
        return []
    finally:
        cursor.close()
        conn.close()

def verificar_usuario(username, password):
    """Verifica credenciales de usuario usando la tabla administrador"""
    conn = get_connection()
    if not conn:
        return None
    
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT administrador_id, usuario 
            FROM administrador 
            WHERE usuario = %s AND contraseña = %s
        """, (username, password))
        row = cursor.fetchone()
        
        if row:
            return {'id': row[0], 'username': row[1]}
        return None
    except psycopg2.Error as e:
        return None
    finally:
        cursor.close()
        conn.close()
