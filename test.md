============================= test session starts ==============================
platform linux -- Python 3.12.3, pytest-8.4.1, pluggy-1.6.0
rootdir: /home/so/projects/myproj/jaql
configfile: pyproject.toml
collected 8 items

tests/test_pipes.py F.FFF                                                [ 62%]
tests/test_runner.py ...                                                 [100%]

=================================== FAILURES ===================================
_______________________________ test_pipe_select _______________________________

    def test_pipe_select():
        """Test select operation filters records correctly"""
        records = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 15},
            {"name": "Charlie", "age": 25}
        ]
    
        # Test age filter
        result = pipe_select(records, "age >= 18")
>       assert len(result) == 2
E       assert 0 == 2
E        +  where 0 = len([])

tests/test_pipes.py:18: AssertionError
_______________________________ test_pipe_derive _______________________________

    def test_pipe_derive():
        """Test derive operation adds computed fields"""
        records = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 15}
        ]
    
        derivations = {
            "is_adult": "age >= 18",
            "name_length": "len(name)"
        }
    
        result = pipe_derive(records, derivations)
        assert len(result) == 2
    
        # Check Alice (adult)
        alice = result[0]
        assert alice["name"] == "Alice"
        assert alice["age"] == 30
>       assert alice["is_adult"] == True
E       assert False == True

tests/test_pipes.py:69: AssertionError
_____________________________ test_apply_pipeline ______________________________

    def test_apply_pipeline():
        """Test complete pipeline execution"""
        data = [
            {"name": "Alice", "age": 30, "email": "alice@example.com"},
            {"name": "Bob", "age": 15, "email": "bob@example.com"},
            {"name": "Charlie", "age": 25, "email": "charlie@example.com"}
        ]
    
        pipeline = [
            {"select": "age >= 18"},
            {"derive": {"is_adult": "age >= 18"}},
            {"project": ["name", "email", "is_adult"]}
        ]
    
        result = apply_pipeline(data, pipeline)
    
        # Should have 2 adults (Alice and Charlie)
>       assert len(result) == 2
E       assert 0 == 2
E        +  where 0 = len([])

tests/test_pipes.py:96: AssertionError
______________________ test_apply_pipeline_single_record _______________________

    def test_apply_pipeline_single_record():
        """Test pipeline with single dict input"""
        data = {"name": "Alice", "age": 30, "email": "alice@example.com"}
    
        pipeline = [
            {"derive": {"is_adult": "age >= 18"}},
            {"project": ["name", "is_adult"]}
        ]
    
        result = apply_pipeline(data, pipeline)
    
        assert len(result) == 1
>       assert result[0] == {"name": "Alice", "is_adult": True}
E       AssertionError: assert {'is_adult': ...ame': 'Alice'} == {'is_adult': ...ame': 'Alice'}
E         
E         Omitting 1 identical items, use -vv to show
E         Differing items:
E         {'is_adult': False} != {'is_adult': True}
E         Use -v to get more diff

tests/test_pipes.py:115: AssertionError
=========================== short test summary info ============================
FAILED tests/test_pipes.py::test_pipe_select - assert 0 == 2
FAILED tests/test_pipes.py::test_pipe_derive - assert False == True
FAILED tests/test_pipes.py::test_apply_pipeline - assert 0 == 2
FAILED tests/test_pipes.py::test_apply_pipeline_single_record - AssertionErro...
========================= 4 failed, 4 passed in 0.06s ==========================
