package me.rahul.druid;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import org.apache.druid.initialization.DruidModule;

import java.util.List;

public class CustomExtensionModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
   return ImmutableList.of(
        new SimpleModule(getClass().getSimpleName()).registerSubtypes(
          new NamedType(CustomJsonInputFormat.class, CustomJsonInputFormat.TYPE_KEY)
        )
    );
  }

  @Override
  public void configure(Binder binder)
  {
  }
}
