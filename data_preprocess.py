import argparse
import json
import os
from pathlib import Path
import sqlite3
from tqdm import tqdm
import shutil
from glob import glob

from utils.linking_process import SpiderEncoderV2Preproc
from utils.pretrained_embeddings import GloVe
from utils.datasets.spider import load_tables


def schema_linking_producer(test, train, table, db, dataset_dir, compute_cv_link=True, compress=False, use_meaning=False):
    """
    Perform schema linking using compressed keywords (adapted for optimized SpiderEncoderV2Preproc)
    """
    # Load compressed keyword data (stored separately for train/dev)
    compressed_data = {
        'test': {},  # Stores compressed data for dev set
        'train': {}  # Stores compressed data for train set
    }
    
    if compress:
        # Compressed file storage directory and paths
        compressed_dir = os.path.join(dataset_dir, "compressed_results")
        dev_compressed_path = os.path.join(compressed_dir, "dev_compressed_columns.jsonl")
        train_compressed_path = os.path.join(compressed_dir, "train_compressed_columns.jsonl")
        
        print(f"[INFO] Compressed file directory: {compressed_dir}")
        
        # Load dev compressed data
        if os.path.exists(dev_compressed_path):
            with open(dev_compressed_path, "r", encoding="utf-8") as f:
                for idx, line in enumerate(f):
                    try:
                        compressed_item = json.loads(line)
                        compressed_data['test'][idx] = {
                            "columns": compressed_item["compressed_column"],
                            "ids": compressed_item["compressed_id"]
                        }
                    except json.JSONDecodeError:
                        print(f"[WARNING] Line {idx+1} in dev compressed file has format error, skipped")
            print(f"[INFO] Loaded dev compressed data, total {len(compressed_data['test'])} valid records")
        else:
            print(f"[WARNING] Dev compressed file {dev_compressed_path} not found, will disable compression mode")
            compress = False
        
        # Load train compressed data (only if compress is still True)
        if compress and os.path.exists(train_compressed_path):
            with open(train_compressed_path, "r", encoding="utf-8") as f:
                for idx, line in enumerate(f):
                    try:
                        compressed_item = json.loads(line)
                        compressed_data['train'][idx] = {
                            "columns": compressed_item["compressed_column"],
                            "ids": compressed_item["compressed_id"]
                        }
                    except json.JSONDecodeError:
                        print(f"[WARNING] Line {idx+1} in train compressed file has format error, skipped")
            print(f"[INFO] Loaded train compressed data, total {len(compressed_data['train'])} valid records")
        elif compress:
            print(f"[WARNING] Train compressed file {train_compressed_path} not found, will disable compression mode")
            compress = False

    # Load original data
    test_data = json.load(open(os.path.join(dataset_dir, test), encoding="utf-8"))
    train_data = json.load(open(os.path.join(dataset_dir, train), encoding="utf-8"))

    # Verify if compressed data matches original data quantity
    if compress:
        if len(compressed_data['test']) != len(test_data):
            print(f"[WARNING] Dev compressed data count({len(compressed_data['test'])}) doesn't match original data count({len(test_data)}), may cause index misalignment")
        if len(compressed_data['train']) != len(train_data):
            print(f"[WARNING] Train compressed data count({len(compressed_data['train'])}) doesn't match original data count({len(train_data)}), may cause index misalignment")

    # Load database schemas
    schemas, _ = load_tables([os.path.join(dataset_dir, table)])

    # Establish database connections
    for db_id, schema in tqdm(schemas.items(), desc="Establishing database connections"):
        sqlite_path = Path(dataset_dir) / db / db_id / f"{db_id}.sqlite"
        if not sqlite_path.exists():
            print(f"Warning: Database file {sqlite_path} not found, skipped")
            continue
        with sqlite3.connect(str(sqlite_path)) as source:
            dest = sqlite3.connect(':memory:')
            dest.row_factory = sqlite3.Row
            source.backup(dest)
        schema.connection = dest

    # Initialize linking processor (pass compression parameters and data)
    word_emb = GloVe(kind='42B', lemmatize=True)
    linking_processor = SpiderEncoderV2Preproc(
        dataset_dir,
        min_freq=4,
        max_count=5000,
        include_table_name_in_column=False,
        word_emb=word_emb,
        fix_issue_16_primary_keys=True,
        compute_sc_link=True,
        compute_cv_link=compute_cv_link,
        use_meaning=use_meaning,
        compress=compress,  # Pass compression mode switch
        compressed_data=compressed_data  # Pass compressed data
    )

    # Build schema-linking (using original items directly, compression logic handled internally by processor)
    for data, section in zip([test_data, train_data], ['test', 'train']):
        for item in tqdm(data, desc=f"Processing {section} set schema linking"):
            db_id = item["db_id"]
            if db_id not in schemas:
                tqdm.write(f"Warning: Database {db_id} not found in schemas, skipped")
                continue
            schema = schemas[db_id]

            # Use original item directly, compression data usage controlled internally by processor
            to_add, validation_info = linking_processor.validate_item(item, schema, section)
            if to_add:
                linking_processor.add_item(item, schema, section, validation_info)

    # Save results
    linking_processor.save()
    print(f"Schema linking results saved to {linking_processor.data_dir}")


def bird_pre_process(bird_dir, with_evidence=False):
    # Keep original implementation unchanged
    import glob
    import shutil
    from pathlib import Path

    new_db_path = os.path.join(bird_dir, "databases")
    os.makedirs(new_db_path, exist_ok=True)

    train_files = glob.glob(os.path.join(bird_dir, 'train/train_databases', '**', '*.sqlite'), recursive=True)
    dev_files = glob.glob(os.path.join(bird_dir, 'dev/dev_databases', '**', '*.sqlite'), recursive=True)

    print("[DEBUG] Found train .sqlite files:")
    for f in train_files:
        print("  ", f)

    print("[DEBUG] Found dev .sqlite files:")
    for f in dev_files:
        print("  ", f)

    for file in train_files + dev_files:
        filename = os.path.basename(file)
        dest = os.path.join(new_db_path, filename)
        shutil.copyfile(file, dest)

    def json_preprocess(data_jsons):
        new_datas = []
        for data_json in data_jsons:
            if with_evidence and "evidence" in data_json and len(data_json["evidence"]) > 0:
                data_json['question'] = (data_json['question'] + " " + data_json["evidence"]).strip()
            question = data_json['question']
            tokens = []
            for token in question.split(' '):
                if len(token) == 0:
                    continue
                if token[-1] in ['?', '.', ':', ';', ','] and len(token) > 1:
                    tokens.extend([token[:-1], token[-1:]])
                else:
                    tokens.append(token)
            data_json['question_toks'] = tokens
            data_json['query'] = data_json['SQL']
            new_datas.append(data_json)
        return new_datas

    output_dev = 'dev.json'
    output_train = 'train.json'
    with open(os.path.join(bird_dir, 'dev/dev.json')) as f:
        data_jsons = json.load(f)
        with open(os.path.join(bird_dir, output_dev), 'w') as wf:
            json.dump(json_preprocess(data_jsons), wf, indent=4)

    with open(os.path.join(bird_dir, 'train/train.json')) as f:
        data_jsons = json.load(f)
        with open(os.path.join(bird_dir, output_train), 'w') as wf:
            json.dump(json_preprocess(data_jsons), wf, indent=4)

    shutil.copy(os.path.join(bird_dir, 'dev/dev.sql'), bird_dir)
    shutil.copy(os.path.join(bird_dir, 'train/train_gold.sql'), bird_dir)

    tables = []
    with open(os.path.join(bird_dir, 'dev/dev_tables.json')) as f:
        tables.extend(json.load(f))
    with open(os.path.join(bird_dir, 'train/train_tables.json')) as f:
        tables.extend(json.load(f))
    with open(os.path.join(bird_dir, 'tables.json'), 'w') as f:
        json.dump(tables, f, indent=4)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_dir", type=str, default="./dataset/spider")
    parser.add_argument("--data_type", type=str, choices=["spider", "bird", "cosc304"], default="spider")
    parser.add_argument("--compress", action="store_true", help="Use compressed keywords for schema linking")
    parser.add_argument("--use_meaning", action="store_true", help="Enable word meaning matching")
    args = parser.parse_args()

    # Print configuration information
    print(f"Data directory: {args.data_dir}")
    print(f"Data type: {args.data_type}")
    print(f"Use compressed keywords: {'Yes' if args.compress else 'No'}")
    print(f"Enable word meaning matching: {'Yes' if args.use_meaning else 'No'}")

    data_type = args.data_type
    if data_type == "spider":
        spider_dir = args.data_dir
        split1 = "train_spider.json"
        split2 = "train_others.json"
        total_train = []
        for item in json.load(open(os.path.join(spider_dir, split1), encoding="utf-8")):
            total_train.append(item)
        for item in json.load(open(os.path.join(spider_dir, split2), encoding="utf-8")):
            total_train.append(item)
        with open(os.path.join(spider_dir, 'train_spider_and_others.json'), 'w', encoding="utf-8") as f:
            json.dump(total_train, f, ensure_ascii=False)

        schema_linking_producer(
            "dev.json", 
            "dev.json", 
            "tables.json", 
            "database", 
            spider_dir,
            compute_cv_link=True,
            compress=args.compress,
            use_meaning=args.use_meaning
        )
    elif data_type == "bird":
        bird_dir = './dataset/bird'
        bird_pre_process(bird_dir, with_evidence=True)
        schema_linking_producer(
            "dev.json", 
            "train.json", 
            "tables.json", 
            "databases", 
            bird_dir, 
            compute_cv_link=False,
            compress=args.compress,
            use_meaning=args.use_meaning
        )
    elif data_type == "cosc304":
        cosc304_dir = args.data_dir
        schema_linking_producer(
            "dev.json",
            "train.json",
            "tables.json",
            "database",
            cosc304_dir,
            compute_cv_link=False,
            compress=args.compress,
            use_meaning=args.use_meaning
        )