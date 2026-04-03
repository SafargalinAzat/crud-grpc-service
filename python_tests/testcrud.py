#!/usr/bin/env python3
import grpc
import service_pb2
import service_pb2_grpc
import time

def main():

    channel = grpc.insecure_channel('localhost:8080')
    stub = service_pb2_grpc.KeyValueServiceStub(channel)
    

    print("\n1. Тест PUT")
    test_key = "python:1"
    test_value = b"Hello"
    
    put_request = service_pb2.PutRequest(key=test_key, value=test_value)
    try:
        put_response = stub.Put(put_request)
        print(f"PUT успешен: success={put_response.success}")
        if put_response.error_message:
            print(f"Ошибка: {put_response.error_message}")
    except Exception as e:
        print(f"PUT failed: {e}")
        return


    print("\n2. Тест GET")
    get_request = service_pb2.GetRequest(key=test_key)
    try:
        get_response = stub.Get(get_request)
        if get_response.found:
            print(f"GET успешен: value={get_response.value}")
        else:
            print(f"GET: ключ не найден")
    except Exception as e:
        print(f"GET failed: {e}")
    
    print("\n3. Тест PUT (обновление)")
    new_value = b"New Value"
    put_request2 = service_pb2.PutRequest(key=test_key, value=new_value)
    try:
        put_response2 = stub.Put(put_request2)
        print(f"PUT обновление успешно: success={put_response2.success}")
    except Exception as e:
        print(f"PUT обновление failed: {e}")
    
    print("\n4. GET после обновления")
    get_request2 = service_pb2.GetRequest(key=test_key)
    try:
        get_response2 = stub.Get(get_request2)
        if get_response2.found:
            print(f"GET успешен: value={get_response2.value}")
        else:
            print(f"GET: ключ не найден")
    except Exception as e:
        print(f"GET failed: {e}")
    
    print("\n5. COUNT")
    count_request = service_pb2.CountRequest()
    try:
        count_response = stub.Count(count_request)
        print(f"COUNT: {count_response.count} записей в базе")
    except Exception as e:
        print(f"COUNT failed: {e}")
    

    print("\n6. Тест RANGE")

    for i in range(1, 6):
        key = f"test:range:{i}"
        value = f"value_{i}".encode()
        stub.Put(service_pb2.PutRequest(key=key, value=value))
    
    range_request = service_pb2.RangeRequest(key_since="test:range:1", key_to="test:range:5")
    try:
        print("   Результаты range:")
        count_range = 0
        for response in stub.Range(range_request):
            print(f"     - key={response.key}, value={response.value}")
            count_range += 1
        print(f"RANGE вернул {count_range} записей")
    except Exception as e:
        print(f"RANGE failed: {e}")
    

    print("\n7. Тест DELETE")
    delete_request = service_pb2.DeleteRequest(key=test_key)
    try:
        delete_response = stub.Delete(delete_request)
        print(f"DELETE: success={delete_response.success}, found={delete_response.found}")
    except Exception as e:
        print(f"DELETE failed: {e}")

    time.sleep(1)
    

    print("\n8. Тест GET после DELETE")
    get_request3 = service_pb2.GetRequest(key=test_key)
    try:
        get_response3 = stub.Get(get_request3)
        if not get_response3.found:
            print(f"GET: ключ успешно удален (не найден)")
        else:
            print(f"GET: ключ все еще существует")
    except Exception as e:
        print(f"GET failed: {e}")
    
    print("\n9. Финальный COUNT")
    try:
        final_count = stub.Count(count_request)
        print(f"Итого записей в базе: {final_count.count}")
    except Exception as e:
        print(f"COUNT failed: {e}")
    

if __name__ == "__main__":
    main()