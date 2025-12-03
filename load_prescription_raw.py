import json
import psycopg2

conn = psycopg2.connect(
    dbname="dosage_limits",
    user="titan",
    password="titan",
    host="localhost",
    port=5432
)
cursor = conn.cursor()

count = 0
with open('dataset/output.jsonl', 'r') as f:  # Changed to output.jsonl
    for line in f:
        data = json.loads(line.strip())
        
        prescription_id = data['prescription_id']
        pharmacy_name = data['pharmacy_name']
        pharmacy_code = data['pharmacy_code']
        prescription_date = data['prescription_date']
        patient_age = data['patient_age']
        patient_gender = data['patient_gender']
        
        for item in data['prescription_items']:
            dosage_fields_list = item.get('dosage_fields', [{}])
            
            for dosage_seq, df in enumerate(dosage_fields_list, start=1):
                row = (
                    prescription_id, pharmacy_name, pharmacy_code, prescription_date,
                    patient_age, patient_gender, item['seq'], item['code'],
                    item['drug'], item['form'], item['route'], item['original_direction'],
                    item['additional_instructions'], item['target_direction'],
                    dosage_seq, df.get('text'), df.get('strength'), df.get('strength_max'),
                    df.get('strength_unit'), df.get('strength_numerator'),
                    df.get('strength_numerator_unit'), df.get('strength_denominator'),
                    df.get('strength_denominator_unit'), df.get('dosage'),
                    df.get('dosage_max'), df.get('dosage_unit'), df.get('frequency'),
                    df.get('frequency_max'), df.get('period'), df.get('period_max'),
                    df.get('period_unit'), df.get('duration'), df.get('duration_max'),
                    df.get('duration_unit'), df.get('as_needed'), df.get('indication')
                )
                
                cursor.execute("""
                    INSERT INTO prescription_items_raw (
                        prescription_id, pharmacy_name, pharmacy_code, prescription_date,
                        patient_age, patient_gender, seq, code, drug, form, route,
                        original_direction, additional_instructions, target_direction,
                        dosage_field_seq, text, strength, strength_max, strength_unit,
                        strength_numerator, strength_numerator_unit, strength_denominator,
                        strength_denominator_unit, dosage, dosage_max, dosage_unit,
                        frequency, frequency_max, period, period_max, period_unit,
                        duration, duration_max, duration_unit, as_needed, indication
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                              %s, %s, %s, %s, %s, %s)
                """, row)
        
        count += 1
        if count % 1000 == 0:
            conn.commit()
            print(f"Loaded {count} prescriptions")

conn.commit()
cursor.close()
conn.close()

print(f"Data loaded successfully! Total: {count} prescriptions")
