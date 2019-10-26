import pywren_ibm_cloud as pywren

def map_function(obj):
    print('Bucket: {}'.format(obj.bucket))
    print('Key: {}'.format(obj.key))
    print('Partition num: {}'.format(obj.part))
    word_map = {}

    lines = obj.data_stream.read().splitlines()

    for line in lines:
        words = line.decode('utf-8').split()
        for word in words:
            count = word_map.get(word, 0)
            word_map[word] = count + 1


    return word_map


def reduce_function(results,ibm_cos):
    final_result = {}
    for count in results:
        for word in count:
            final_result[word] = final_result.get(count[word], 0)
            final_result[word] += count[word]
    return final_result

def save_function(results,bucket,file,ibm_cos):
    ibm_cos.put_object(Bucket=bucket, Key=file,
                      Body=str(results),
                      Metadata={'Content-Type': 'plain/text'})
    return {"status":"ok"}


if __name__ == '__main__':

    config = get_config()

    ibmcf = pywren.ibm_cf_executor(config=config)

    iterdata = ['cos://urv-mapreduce-exercise/prueba.txt']
    ibmcf.map_reduce(map_function, iterdata, reduce_function, chunk_size=1024 * 4 * 2)
    results = ibmcf.get_result()

    reduce_params =  {'results':results,
                    'bucket':'urv-mapreduce-exercise',
                   'file': 'pyWren_output.txt'}
    ibmcf.call_async(save_function,reduce_params)
    print(ibmcf.get_result())


