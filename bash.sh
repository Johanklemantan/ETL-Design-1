echo "Start ELT Process"
echo "Start Migration to Database Sink"
python migrasi_source.py
echo "Start Transforming Data"
python transform.py
echo "ELT Finished." 