#!/usr/bin/env python
import argparse
import os
import numpy as np
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq
import random
import string

# ---------- Helpers ----------
MX_STATES = [
    "Aguascalientes","Baja California","Baja California Sur","Campeche","Coahuila","Colima","Chiapas",
    "Chihuahua","Ciudad de México","Durango","Guanajuato","Guerrero","Hidalgo","Jalisco","México",
    "Michoacán","Morelos","Nayarit","Nuevo León","Oaxaca","Puebla","Querétaro","Quintana Roo",
    "San Luis Potosí","Sinaloa","Sonora","Tabasco","Tamaulipas","Tlaxcala","Veracruz","Yucatán","Zacatecas"
]

ROLES = ["Medico", "Tecnico", "Recepcion", "Caja", "Gerente", "Limpieza", "Soporte"]
PAYMENT_METHODS = ["EFECTIVO","TARJETA","TRANSFERENCIA","VALES"]
STATUS = ["completada","no_show","cancelada"]

SERVICIOS = [
    ("CBC","Hematología","Biometría hemática"),
    ("GLU","Química","Glucosa"),
    ("LIP","Química","Perfil lipídico"),
    ("TSH","Hormonas","TSH"),
    ("T4F","Hormonas","T4 libre"),
    ("PCR","Marcadores","PCR"),
    ("RXT","Imagen","Rayos X tórax"),
    ("USG","Imagen","Ultrasonido general"),
    ("USO","Imagen","Ultrasonido obstétrico"),
    ("MRI","Imagen","Resonancia magnética"),
    ("CTC","Imagen","Tomografía computarizada"),
    ("URI","Análisis","Examen general de orina"),
    ("COP","Análisis","Coproparasitoscópico"),
    ("AGP","Marcadores","Antígeno prostático"),
    ("HBA","Marcadores","HbA1c"),
    ("COVID","Marcadores","Antígeno SARS‑CoV‑2"),
    ("VITD","Marcadores","Vitamina D"),
    ("FER","Marcadores","Ferritina"),
    ("AST","Química","AST"),
    ("ALT","Química","ALT"),
    ("CREA","Química","Creatinina"),
    ("URE","Química","Urea"),
    ("COL","Química","Colesterol total"),
    ("TRIG","Química","Triglicéridos"),
    ("PLT","Hematología","Plaquetas"),
    ("WBC","Hematología","Leucocitos"),
    ("HGB","Hematología","Hemoglobina"),
    ("ECO","Imagen","Ecocardiograma"),
    ("PAP","Citología","Papanicolaou"),
    ("MAMO","Imagen","Mastografía"),
    ("DENS","Imagen","Densitometría"),
    ("ESPI","Funcional","Espirometría"),
    ("AUD","Funcional","Audiometría"),
    ("VIS","Funcional","Visiometría"),
    ("HOL","Funcional","Holter"),
    ("MAPA","Funcional","MAPA"),
    ("GLUC","Funcional","Curva de tolerancia"),
    ("EPCG","Funcional","Electrocardiograma"),
    ("STREP","Marcadores","Streptococcus A")
]

INSUMOS = [
    ("TUBO_ROJO","Tubo vacutainer rojo", "pieza", 8, 2.5),
    ("TUBO_LILA","Tubo EDTA lila", "pieza", 8, 2.7),
    ("SWAB","Hisopo estéril", "pieza", 5, 1.2),
    ("AGUJA","Aguja 21G", "pieza", 7, 0.9),
    ("GUANTE_N","Guante nitrilo", "par", 6, 0.35),
    ("ALCOHOL","Alcohol isopropílico", "lt", 10, 3.0),
    ("GASA","Gasa estéril", "pieza", 6, 0.2),
    ("REACT_HBA1C","Reactivo HbA1c", "kit", 12, 22.0),
    ("REACT_TSH","Reactivo TSH", "kit", 12, 25.0),
    ("REACT_PCR","Reactivo PCR", "kit", 15, 28.0),
    ("REACT_COVID","Reactivo Antígeno", "kit", 10, 18.0),
    ("PLACA_RX","Placa RX", "pieza", 20, 12.0),
    ("GEL_US","Gel ultrasonido", "lt", 9, 4.0),
    ("PAP_KIT","Kit Papanicolaou", "kit", 12, 6.5),
    ("AGUA_DES","Agua destilada", "lt", 10, 1.0),
    ("TORUN","Torundas", "bolsa", 6, 0.8),
    ("TERMOP","Papel termo", "rollo", 14, 2.2),
    ("MASC","Mascarilla", "pieza", 6, 0.15),
    ("CUBRE","Cubre bocas", "pieza", 6, 0.12),
    ("BATA","Bata desechable", "pieza", 7, 1.6)
]

def rand_code(prefix, n=6):
    s = ''.join(random.choices(string.ascii_uppercase + string.digits, k=n))
    return f"{prefix}-{s}"

def write_parquet(df, path):
    table = pa.Table.from_pandas(df)
    pq.write_table(table, path)

def write_csv_sample(df, path, n=5000):
    df.head(n).to_csv(path, index=False, encoding='utf-8')

# ---------- Main generator ----------
def main(args):
    random.seed(args.seed); np.random.seed(args.seed)
    fake = Faker("es_MX")
    out = args.output_dir
    os.makedirs(out, exist_ok=True)

    # --- dim_sucursal ---
    # 100 branches with random state/municipality and coordinates
    municipios_seed = ["Centro","Norte","Sur","Oriente","Poniente","Industrial","Universidad","Roma","Del Valle","Juárez"]
    sucursales = []
    for i in range(args.n_sucursales):
        state = random.choice(MX_STATES)
        muni = random.choice(municipios_seed)
        name = f"Sucursal {i+1:03d}"
        code = f"SUC{i+1:03d}"
        lat = round(np.random.uniform(14.5, 32.7), 6)
        lon = round(np.random.uniform(-117.0, -86.5), 6)
        open_date = fake.date_between(start_date="-8y", end_date="-30d")
        sucursales.append((i+1, code, name, state, muni, fake.street_address(), lat, lon, str(open_date)))
    df_suc = pd.DataFrame(sucursales, columns=[
        "sucursal_id","sucursal_code","sucursal_name","estado","municipio","direccion","lat","lon","fecha_apertura"
    ])
    write_parquet(df_suc, os.path.join(out,"dim_sucursal.parquet"))
    write_csv_sample(df_suc, os.path.join(out,"dim_sucursal_sample.csv"))

    # --- dim_servicio --- (cap to n_servicios if smaller than list)
    servicios_list = SERVICIOS[:max(1, min(args.n_servicios, len(SERVICIOS)))]
    df_srv = pd.DataFrame([
        (i+1, s[0], s[1], s[2],
         round(np.random.uniform(120, 1800),2)) for i, s in enumerate(servicios_list)
    ], columns=["servicio_id","servicio_code","categoria","descripcion","precio_base"])
    write_parquet(df_srv, os.path.join(out,"dim_servicio.parquet"))

    # --- dim_insumo ---
    insumos_list = INSUMOS[:max(1, min(args.n_insumos, len(INSUMOS)))]
    df_ins = pd.DataFrame([
        (i+1, x[0], x[1], x[2], x[3], x[4],
         np.random.randint(20,120), # reorder_point
         np.random.randint(80,400)  # reorder_qty
        ) for i,x in enumerate(insumos_list)
    ], columns=["insumo_id","insumo_code","insumo_name","uom","lead_time_dias","costo_unitario","reorder_point","reorder_qty"])
    write_parquet(df_ins, os.path.join(out,"dim_insumo.parquet"))

    # --- dim_cliente ---
    clientes = []
    for i in range(args.n_clientes):
        sex = random.choice(["M","F"])
        birth = fake.date_between(start_date="-85y", end_date="-18y")
        reg = fake.date_between(start_date="-5y", end_date="today")
        clientes.append((i+1, fake.unique.ssn(), fake.first_name_male() if sex=="M" else fake.first_name_female(),
                         fake.last_name(), sex, str(birth), str(reg),
                         random.choice(["Publico","Empresa","Convenio"])))
    df_cli = pd.DataFrame(clientes, columns=[
        "cliente_id","curp_like","nombre","apellido","sexo","fecha_nacimiento","fecha_registro","segmento"
    ])
    write_parquet(df_cli, os.path.join(out,"dim_cliente.parquet"))

    # --- dim_empleado ---
    empleados = []
    for i in range(args.n_empleados):
        suc = np.random.randint(1, args.n_sucursales+1)
        role = random.choice(ROLES)
        hire = fake.date_between(start_date="-6y", end_date="-30d")
        shift = random.choice(["Mañana","Tarde","Mixto"])
        empleados.append((i+1, rand_code("EMP"), fake.name(), role, suc, str(hire), shift, np.random.randint(9000,28000)))
    df_emp = pd.DataFrame(empleados, columns=[
        "empleado_id","empleado_code","empleado_name","rol","sucursal_id","fecha_contratacion","turno","salario_mensual"
    ])
    write_parquet(df_emp, os.path.join(out,"dim_empleado.parquet"))

    # --- fact_venta_cita ---
    # Distribute over last 18 months
    end = datetime.now().date() - timedelta(days=1)
    start = end - timedelta(days=540)
    dates = pd.date_range(start, end, freq="D")
    # approximate per-day volume
    per_day = max(1, args.n_ventas // max(1,len(dates)))
    records = []
    for d in dates:
        n = int(np.random.poisson(lam=per_day))
        for _ in range(n):
            suc = np.random.randint(1, args.n_sucursales+1)
            srv = np.random.randint(1, len(df_srv)+1)
            emp = np.random.randint(1, args.n_empleados+1)
            cli = np.random.randint(1, args.n_clientes+1)
            dt = datetime.combine(d.date(), datetime.min.time()) + timedelta(minutes=int(np.random.uniform(7*60, 20*60)))
            price = df_srv.loc[srv-1,"precio_base"]
            disc = round(max(0.0, np.random.normal(loc=0.05, scale=0.05)), 2) if random.random()<0.35 else 0.0
            pm = random.choice(PAYMENT_METHODS)
            st = np.random.choice(STATUS, p=[0.88,0.07,0.05])
            wait = max(0, int(np.random.normal(loc=24, scale=10)))
            dur = max(5, int(np.random.normal(loc=35, scale=12)))
            records.append((
                len(records)+1, dt.isoformat(), suc, cli, emp, srv,
                round(price*(1-disc),2), disc, pm, st, wait, dur
            ))
            if len(records) >= args.n_ventas:
                break
        if len(records) >= args.n_ventas:
            break
    df_sales = pd.DataFrame(records, columns=[
        "venta_id","fecha_hora","sucursal_id","cliente_id","empleado_id","servicio_id",
        "precio_final","descuento","metodo_pago","estatus","espera_min","duracion_min"
    ])
    write_parquet(df_sales, os.path.join(out,"fact_venta_cita.parquet"))

    # --- fact_inventario_mov ---
    inv_records = []
    for i in range(args.n_inv_mov):
        suc = np.random.randint(1, args.n_sucursales+1)
        ins = np.random.randint(1, len(df_ins)+1)
        movement = np.random.choice(["IN","OUT","ADJ"], p=[0.25,0.7,0.05])
        qty = int(max(1, np.random.normal(loc=6 if movement!="IN" else 18, scale=4)))
        dt = start + timedelta(days=int(np.random.uniform(0, (end-start).days)), minutes=int(np.random.uniform(0, 24*60)))
        unit_cost = float(df_ins.loc[ins-1,"costo_unitario"]) * np.random.uniform(0.95, 1.08)
        inv_records.append((i+1, dt.isoformat(), suc, ins, movement, qty, round(unit_cost,2), rand_code("DOC",5)))
    df_inv = pd.DataFrame(inv_records, columns=[
        "mov_id","fecha_hora","sucursal_id","insumo_id","tipo_mov","cantidad","costo_unitario","doc_ref"
    ])
    write_parquet(df_inv, os.path.join(out,"fact_inventario_mov.parquet"))

    # --- snap_inventario_diario --- (per a subset of days to keep ~120k rows)
    snap_days = min(max(1, args.dias_snapshot), 30)
    snap_records = []
    chosen_days = [end - timedelta(days=i) for i in range(snap_days)][::-1]
    for suc in range(1, args.n_sucursales+1):
        for ins in range(1, len(df_ins)+1):
            base = np.random.randint(50, 300)
            for d in chosen_days:
                # add random walk
                base = max(0, base + np.random.randint(-8, 8))
                snap_records.append((len(snap_records)+1, d.isoformat(), suc, ins, int(base)))
    df_snap = pd.DataFrame(snap_records, columns=["snap_id","fecha","sucursal_id","insumo_id","stock_qty"])
    write_parquet(df_snap, os.path.join(out,"snap_inventario_diario.parquet"))

    # --- CSV samples for quick inspection ---
    write_csv_sample(df_srv, os.path.join(out,"dim_servicio_sample.csv"))
    write_csv_sample(df_ins, os.path.join(out,"dim_insumo_sample.csv"))
    write_csv_sample(df_cli, os.path.join(out,"dim_cliente_sample.csv"))
    write_csv_sample(df_emp, os.path.join(out,"dim_empleado_sample.csv"))
    write_csv_sample(df_sales, os.path.join(out,"fact_venta_cita_sample.csv"))
    write_csv_sample(df_inv, os.path.join(out,"fact_inventario_mov_sample.csv"))
    write_csv_sample(df_snap, os.path.join(out,"snap_inventario_diario_sample.csv"))

    print("Synthetic dataset generated at:", out)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=str, default="data_generator/output")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--n-sucursales", type=int, default=100)
    parser.add_argument("--n-clientes", type=int, default=150000)
    parser.add_argument("--n-empleados", type=int, default=8000)
    parser.add_argument("--n-servicios", type=int, default=40)
    parser.add_argument("--n-insumos", type=int, default=200)
    parser.add_argument("--n-ventas", type=int, default=650000)
    parser.add_argument("--n-inv-mov", type=int, default=150000)
    parser.add_argument("--dias-snapshot", type=int, default=6)
    args = parser.parse_args()
    main(args)
