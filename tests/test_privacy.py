from fastapi.testclient import TestClient

def test_remote_pipeline_requires_operator_authorization(monkeypatch):
    monkeypatch.setenv('ANTHROPIC_API_KEY','test-not-real')
    # Validation is not exercised: rejected remote requests must stop before it.
    import types,sys
    validator=types.ModuleType('src.validator')
    def forbidden_validation(*args,**kwargs): raise AssertionError('Unauthenticated request reached validation')
    validator.validate_user_signup=forbidden_validation
    monkeypatch.setitem(sys.modules,'src.validator',validator)
    from src.approval_api import app
    client=TestClient(app,base_url='https://public.example')
    assert client.get('/health').status_code==200
    assert client.get('/events').status_code==403
    assert client.post('/approve',json={}).status_code==403
    monkeypatch.setenv('ETL_ADMIN_TOKEN','operator-secret-for-test')
    assert client.get('/unknown',headers={'Authorization':'Bearer operator-secret-for-test'}).status_code==404
