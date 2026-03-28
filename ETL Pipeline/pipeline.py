import generate_data as extract
import transform
import load

def pipeline():
    extract.extract()
    transform.transform()
    load.loader()

if __name__ == '__main__':
    pipeline()
