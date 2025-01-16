#pragma once
#include <cstdint>
#include <string>
#include <memory>
#include <stdexcept>
#include <map>
#include <nlohmann/json.hpp>

class Namespace {
private:
    uint64_t namespace_handle;

public:
    Namespace(uint64_t handle) : namespace_handle(handle) {}
    
    uint64_t getId() const;
    void dropData();
    void alterSchema(const std::string& schema);
    std::map<std::string, uint64_t> mutate(const std::string& mutations);
    std::string query(const std::string& queryStr);
};

class Engine {
private:
    uint64_t engine_handle;

public:
    Engine(const std::string& dataDir);
    ~Engine();
    
    Namespace createNamespace();
    Namespace getNamespace(uint64_t nsID);
    void dropAll();
    void load(const std::string& schemaPath, const std::string& dataPath);
    void loadData(const std::string& dataDir);
    void close();
};