import os, time
import xml.etree.cElementTree as ET
import random, string
import zipfile
from io import BytesIO
import csv
from multiprocessing import Pool as ProcessPool


'''

1. Создает 50 zip-архивов, 
в каждом 100 xml файлов со случайными данными следующей структуры:

<root>
<var name=’id’ value=’<случайное уникальное строковое значение>’/>
<var name=’level’ value=’<случайное число от 1 до 100>’/>
<objects>
    <object name=’<случайное строковое значение>’/>
    <object name=’<случайное строковое значение>’/>…
</objects>
</root>

В тэге objects случайное число (от 1 до 10) вложенных тэгов object.

2. Обрабатывает директорию с полученными zip архивами, 
разбирает вложенные xml файлы и формирует 2 csv файла:
Первый: id, level - по одной строке на каждый xml 
файлВторой: id, object_name - по отдельной строке для каждого тэга object 
(получится от 1 до 10 строк на каждый xml файл)

Очень желательно сделать так, чтобы задание 2 эффективно использовало 
ресурсы многоядерного процессора. Пожалуйста загрузите на гитхаб)

'''


def develop_zip_file(zip_file):
    """read all files in zip file and return two lists"""
    CSV_LIST1 = []
    CSV_LIST2 = []
    with zipfile.ZipFile(f'./xmls/{zip_file}') as zf:
        for file in zf.namelist():
            if not file.endswith('.xml'):
                continue
            with zf.open(file) as f:
                root = ET.fromstring(f.read())
                id_value, id_level = "", ""
                object_names = []
                for child in root:
                    if child.tag == 'var':
                        if child.attrib['name'] == 'id':
                            id_value = child.attrib['value']
                        if child.attrib['name'] == 'level':
                            id_level = child.attrib['value']

                    if child.tag == 'objects':
                        for o in child:
                            object_names.append(o.attrib.get('name'))

                CSV_LIST1.append({'id': id_value, 'level': id_level})
                for on in object_names:
                    CSV_LIST2.append({'id': id_value, 'object_name': on})
    return CSV_LIST1, CSV_LIST2

def write_csv(csv_list, csv_file_name_):
    """write csv file from list"""
    keys = csv_list[0].keys()
    with open(csv_file_name_, 'w') as f:
        dict_writer = csv.DictWriter(f, keys)
        dict_writer.writeheader()
        dict_writer.writerows(csv_list)


def develop_dir():
    """ read zip files in dir and develop them in two modes async and ordinary"""
    files_arr = os.listdir("./xmls/")
    zip_files = []
    for n in files_arr:
        if n[-4:] == '.zip':
            zip_files.append(n)

    CSV_LIST1 = []
    CSV_LIST2 = []

    strat_time = time.time()
    with ProcessPool(processes=4) as pool:
        results = pool.map(develop_zip_file, zip_files)
    time_take = time.time() - strat_time
    print("async time take:", time_take)

    for n in results:
        CSV_LIST1 = CSV_LIST1 + n[0]
        CSV_LIST2 = CSV_LIST2 + n[1]
    write_csv(CSV_LIST1, "a1.csv")
    write_csv(CSV_LIST2, "a2.csv")
    CSV_LIST1 = []
    CSV_LIST2 = []


    strat_time = time.time()
    for z in zip_files:
        ret1, ret2 = develop_zip_file(z)
        CSV_LIST1 = CSV_LIST1 + ret1
        CSV_LIST2 = CSV_LIST2 + ret2

    time_take = time.time()-strat_time
    print("time take:",time_take)

    write_csv(CSV_LIST1, "1.csv")
    write_csv(CSV_LIST2, "2.csv")




def randomstring(length=10):
    """ make a random string """
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))

def makexml():
    """ make a xml file """
    root = ET.Element("root")
    ET.SubElement(root, "var", name="id", value=randomstring())
    ET.SubElement(root, "var", name="level", value=str(random.randrange(1, 100)))
    objects = ET.SubElement(root, "objects")
    objectsnum = random.randrange(1 , 10)
    for _ in range(objectsnum):
        ET.SubElement(objects, "object", name=randomstring())

    tree = ET.ElementTree(root)
    bio = BytesIO()
    tree.write(bio)
    return bio.getvalue()

def make_zips():
    """ make a 50 zip files
    Make  ./xmls subdir befor run this func!!!!!!
    """
    for m in range(50):
        with zipfile.ZipFile(f'./xmls/{m}.zip', 'w') as z:
            for n in range(100):
                z.writestr(f'{m}x{n}.xml', makexml())



if __name__ == "__main__":
    make_zips()
    develop_dir()
    print("finish")
