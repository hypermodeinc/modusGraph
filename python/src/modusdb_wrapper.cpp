#include "modusdb_wrapper.hpp"
#include <string>

// Declare the C functions from your Go code
extern "C" {
    uint64_t NewEngineC(const char* dataDir, char** err);
    uint64_t CreateNamespaceC(uint64_t engine, char** err);
    uint64_t GetNamespaceC(uint64_t engine, uint64_t nsID, char** err);
    void DropAllC(uint64_t engine, char** err);
    void LoadC(uint64_t engine, const char* schemaPath, const char* dataPath, char** err);
    void LoadDataC(uint64_t engine, const char* dataDir, char** err);
    void CloseC(uint64_t engine);
    uint64_t GetNamespaceIDC(uint64_t ns);
    void DropDataC(uint64_t ns, char** err);
    void AlterSchemaC(uint64_t ns, const char* schema, char** err);
    void MutateC(uint64_t ns, const char* mutations, char** result, char** err);
    void QueryC(uint64_t ns, const char* query, char** result, char** err);
}

// Helper function to check and throw errors
void checkError(char** err) {
    if (*err != nullptr) {
        std::string error_msg(*err);
        free(*err);
        throw std::runtime_error(error_msg);
    }
}

Engine::Engine(const std::string& dataDir) {
    char* err = nullptr;
    engine_handle = NewEngineC(dataDir.c_str(), &err);
    checkError(&err);
    if (engine_handle == 0) {
        throw std::runtime_error("Failed to create engine");
    }
}

Engine::~Engine() {
    if (engine_handle != 0) {
        close();
    }
}

Namespace Engine::createNamespace() {
    char* err = nullptr;
    uint64_t ns = CreateNamespaceC(engine_handle, &err);
    checkError(&err);
    return Namespace(ns);
}

Namespace Engine::getNamespace(uint64_t nsID) {
    char* err = nullptr;
    uint64_t ns = GetNamespaceC(engine_handle, nsID, &err);
    checkError(&err);
    return Namespace(ns);
}

void Engine::dropAll() {
    char* err = nullptr;
    DropAllC(engine_handle, &err);
    checkError(&err);
}

void Engine::load(const std::string& schemaPath, const std::string& dataPath) {
    char* err = nullptr;
    LoadC(engine_handle, schemaPath.c_str(), dataPath.c_str(), &err);
    checkError(&err);
}

void Engine::loadData(const std::string& dataDir) {
    char* err = nullptr;
    LoadDataC(engine_handle, dataDir.c_str(), &err);
    checkError(&err);
}

void Engine::close() {
    if (engine_handle != 0) {
        CloseC(engine_handle);
        engine_handle = 0;
    }
}

uint64_t Namespace::getId() const {
    return GetNamespaceIDC(namespace_handle);
}

void Namespace::dropData() {
    char* err = nullptr;
    DropDataC(namespace_handle, &err);
    checkError(&err);
}

void Namespace::alterSchema(const std::string& schema) {
    char* err = nullptr;
    AlterSchemaC(namespace_handle, schema.c_str(), &err);
    checkError(&err);
}

std::map<std::string, uint64_t> Namespace::mutate(const std::string& mutations) {
    char* err = nullptr;
    char* result = nullptr;
    
    MutateC(namespace_handle, mutations.c_str(), &result, &err);
    checkError(&err);
    
    if (result == nullptr) {
        throw std::runtime_error("No result returned from mutation");
    }
    
    // Parse JSON string into map
    std::map<std::string, uint64_t> resultMap;
    try {
        auto j = nlohmann::json::parse(result);
        resultMap = j.get<std::map<std::string, uint64_t>>();
    } catch (const nlohmann::json::exception& e) {
        free(result);
        throw std::runtime_error(std::string("Failed to parse mutation result: ") + e.what());
    }
    
    free(result);
    return resultMap;
}

std::string Namespace::query(const std::string& queryStr) {
    char* err = nullptr;
    char* result = nullptr;
    
    QueryC(namespace_handle, queryStr.c_str(), &result, &err);
    checkError(&err);
    
    if (result == nullptr) {
        throw std::runtime_error("No result returned from query");
    }
    
    std::string response(result);
    free(result);
    return response;
}